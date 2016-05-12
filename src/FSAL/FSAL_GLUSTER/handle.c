/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) Red Hat  Inc., 2013
 * Author: Anand Subramanian anands@redhat.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * -------------
 */

#include <fcntl.h>
#include "fsal.h"
#include "gluster_internal.h"
#include "FSAL/fsal_commonlib.h"
#include "fsal_convert.h"
#include "pnfs_utils.h"
#include "nfs_exports.h"
#include "sal_data.h"

/* fsal_obj_handle common methods
 */

/**
 * @brief Implements GLUSTER FSAL objectoperation handle_release
 *
 * Free up the GLUSTER handle and associated data if any
 * Typically free up any members of the struct glusterfs_handle
 */

static void handle_release(struct fsal_obj_handle *obj_hdl)
{
	int rc = 0;
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	fsal_obj_handle_fini(&objhandle->handle);

	if (objhandle->globalfd.glfd) {
		rc = glfs_close(objhandle->globalfd.glfd);
		if (rc) {
			LogCrit(COMPONENT_FSAL,
				"glfs_close returned %s(%d)",
				strerror(errno), errno);
			/* cleanup as much as possible */
		}
	}

	if (objhandle->glhandle) {
		rc = glfs_h_close(objhandle->glhandle);
		if (rc) {
			LogCrit(COMPONENT_FSAL,
				"glfs_h_close returned error %s(%d)",
				strerror(errno), errno);
		}
	}

	gsh_free(objhandle);

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_handle_release);
#endif
}

/**
 * @brief Implements GLUSTER FSAL objectoperation lookup
 */

static fsal_status_t lookup(struct fsal_obj_handle *parent,
			    const char *path, struct fsal_obj_handle **handle)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	struct glusterfs_handle *objhandle = NULL;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(parent, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

#ifdef USE_GLUSTER_SYMLINK_MOUNT
		glhandle = glfs_h_lookupat(glfs_export->gl_fs,
					parenthandle->glhandle, path, &sb, 0);
#else
		glhandle = glfs_h_lookupat(glfs_export->gl_fs,
					parenthandle->glhandle, path, &sb);
#endif
	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid, GLAPI_UUID_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(rc);
		goto out;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &objhandle, vol_uuid);

	*handle = &objhandle->handle;

 out:
	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_lookup);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation readdir
 */

static fsal_status_t read_dirents(struct fsal_obj_handle *dir_hdl,
				  fsal_cookie_t *whence, void *dir_state,
				  fsal_readdir_cb cb, bool *eof)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glfs_fd *glfd = NULL;
	long offset = 0;
	struct dirent *pde = NULL;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	glfd = glfs_h_opendir(glfs_export->gl_fs, objhandle->glhandle);
	if (glfd == NULL)
		return gluster2fsal_error(errno);

	if (whence != NULL)
		offset = *whence;

	glfs_seekdir(glfd, offset);

	while (!(*eof)) {
		struct dirent de;

		rc = glfs_readdir_r(glfd, &de, &pde);
		if (rc == 0 && pde != NULL) {
			/* skip . and .. */
			if ((strcmp(de.d_name, ".") == 0)
			    || (strcmp(de.d_name, "..") == 0)) {
				continue;
			}
			if (!cb(de.d_name, dir_state, glfs_telldir(glfd)))
				goto out;
		} else if (rc == 0 && pde == NULL) {
			*eof = true;
		} else {
			status = gluster2fsal_error(errno);
			goto out;
		}
	}

 out:
	rc = glfs_closedir(glfd);
	if (rc < 0)
		status = gluster2fsal_error(errno);
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_read_dirents);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation create
 */

static fsal_status_t create(struct fsal_obj_handle *dir_hdl,
			    const char *name, struct attrlist *attrib,
			    struct fsal_obj_handle **handle)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	struct glusterfs_handle *objhandle = NULL;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	rc = setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			     &op_ctx->creds->caller_gid,
			     op_ctx->creds->caller_glen,
			     op_ctx->creds->caller_garray);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	/* FIXME: what else from attrib should we use? */
	glhandle =
	    glfs_h_creat(glfs_export->gl_fs, parenthandle->glhandle, name,
			 O_CREAT | O_EXCL, fsal2unix_mode(attrib->mode), &sb);

	rc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid, GLAPI_UUID_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(rc);
		goto out;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &objhandle, vol_uuid);

	*handle = &objhandle->handle;
	*attrib = objhandle->attributes;

 out:
	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_create);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation mkdir
 */

static fsal_status_t makedir(struct fsal_obj_handle *dir_hdl,
			     const char *name, struct attrlist *attrib,
			     struct fsal_obj_handle **handle)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	struct glusterfs_handle *objhandle = NULL;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	rc = setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			     &op_ctx->creds->caller_gid,
			     op_ctx->creds->caller_glen,
			     op_ctx->creds->caller_garray);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	/* FIXME: what else from attrib should we use? */
	glhandle =
	    glfs_h_mkdir(glfs_export->gl_fs, parenthandle->glhandle, name,
			 fsal2unix_mode(attrib->mode), &sb);

	rc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid, GLAPI_UUID_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(rc);
		goto out;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &objhandle, vol_uuid);

	*handle = &objhandle->handle;
	*attrib = objhandle->attributes;

 out:
	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_makedir);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation mknode
 */

static fsal_status_t makenode(struct fsal_obj_handle *dir_hdl,
			      const char *name, object_file_type_t nodetype,
			      fsal_dev_t *dev, struct attrlist *attrib,
			      struct fsal_obj_handle **handle)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	struct glusterfs_handle *objhandle = NULL;
	dev_t ndev = { 0, };
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
	mode_t create_mode;
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	switch (nodetype) {
	case BLOCK_FILE:
		if (!dev)
			return fsalstat(ERR_FSAL_INVAL, 0);
		/* FIXME: This needs a feature flag test? */
		ndev = makedev(dev->major, dev->minor);
		create_mode = S_IFBLK;
		break;
	case CHARACTER_FILE:
		if (!dev)
			return fsalstat(ERR_FSAL_INVAL, 0);
		ndev = makedev(dev->major, dev->minor);
		create_mode = S_IFCHR;
		break;
	case FIFO_FILE:
		create_mode = S_IFIFO;
		break;
	case SOCKET_FILE:
		create_mode = S_IFSOCK;
		break;
	default:
		LogMajor(COMPONENT_FSAL, "Invalid node type in FSAL_mknode: %d",
			 nodetype);
		return fsalstat(ERR_FSAL_INVAL, 0);
	}

	rc = setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			     &op_ctx->creds->caller_gid,
			     op_ctx->creds->caller_glen,
			     op_ctx->creds->caller_garray);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	/* FIXME: what else from attrib should we use? */
	glhandle =
	    glfs_h_mknod(glfs_export->gl_fs, parenthandle->glhandle, name,
			 create_mode | fsal2unix_mode(attrib->mode), ndev, &sb);

	rc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid, GLAPI_UUID_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(rc);
		goto out;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &objhandle, vol_uuid);

	*handle = &objhandle->handle;
	*attrib = objhandle->attributes;

 out:
	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_makenode);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation symlink
 */

static fsal_status_t makesymlink(struct fsal_obj_handle *dir_hdl,
				 const char *name, const char *link_path,
				 struct attrlist *attrib,
				 struct fsal_obj_handle **handle)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	struct glusterfs_handle *objhandle = NULL;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	rc = setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			     &op_ctx->creds->caller_gid,
			     op_ctx->creds->caller_glen,
			     op_ctx->creds->caller_garray);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	/* FIXME: what else from attrib should we use? */
	glhandle =
	    glfs_h_symlink(glfs_export->gl_fs, parenthandle->glhandle, name,
			   link_path, &sb);

	rc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (rc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	rc = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid, GLAPI_UUID_LENGTH);
	if (rc < 0) {
		status = gluster2fsal_error(rc);
		goto out;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &objhandle, vol_uuid);

	*handle = &objhandle->handle;
	*attrib = objhandle->attributes;

 out:
	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_makesymlink);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation readlink
 */

static fsal_status_t readsymlink(struct fsal_obj_handle *obj_hdl,
				 struct gsh_buffdesc *link_content,
				 bool refresh)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	link_content->len = MAXPATHLEN; /* Max link path */
	link_content->addr = gsh_malloc(link_content->len);

	rc = glfs_h_readlink(glfs_export->gl_fs, objhandle->glhandle,
			     link_content->addr, link_content->len);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	if (rc >= MAXPATHLEN) {
		status = gluster2fsal_error(EINVAL);
		goto out;
	}

	/* rc is the number of bytes copied into link_content->addr
	 * without including '\0' character. */
	*(char *)(link_content->addr + rc) = '\0';
	link_content->len = rc + 1;

 out:
	if (status.major != ERR_FSAL_NO_ERROR) {
		gsh_free(link_content->addr);
		link_content->addr = NULL;
		link_content->len = 0;
	}
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_readsymlink);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation getattrs
 */

static fsal_status_t getattrs(struct fsal_obj_handle *obj_hdl)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	glusterfs_fsal_xstat_t buffxstat;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
	struct attrlist *fsalattr;
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	/*
	 * There is a kind of race here when the glfd part of the
	 * FSAL GLUSTER object handle is destroyed during a close
	 * coming in from another NFSv3 WRITE thread which does
	 * cache_inode_open(). Since the context/fd is destroyed
	 * we cannot depend on glfs_fstat assuming glfd is valid.

	 * Fixing the issue by removing the glfs_fstat call here.

	 * So default to glfs_h_stat and re-optimize if a better
	 * way is found - that may involve introducing locks in
	 * the gfapi's for close and getattrs etc.
	 */
	rc = glfs_h_stat(glfs_export->gl_fs,
			 objhandle->glhandle, &buffxstat.buffstat);
	if (rc != 0) {
		if (errno == ENOENT)
			status = gluster2fsal_error(ESTALE);
		else
			status = gluster2fsal_error(errno);

		goto out;
	}

	fsalattr = &objhandle->attributes;
	stat2fsal_attributes(&buffxstat.buffstat, fsalattr);
	if (obj_hdl->type == DIRECTORY)
		buffxstat.is_dir = true;
	else
		buffxstat.is_dir = false;

	status = glusterfs_get_acl(glfs_export, objhandle->glhandle,
				   &buffxstat, fsalattr);

	/*
	 * The error ENOENT is not an expected error for GETATTRS.
	 * Due to this, operations such as RENAME will fail when it
	 * calls GETATTRS on removed file.
	 */
	if (status.minor == ENOENT)
		status = gluster2fsal_error(ESTALE);

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_getattrs);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation setattrs
 */

static fsal_status_t setattrs(struct fsal_obj_handle *obj_hdl,
			      struct attrlist *attrs)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	glusterfs_fsal_xstat_t buffxstat;
	int mask = 0;
	int attr_valid = 0;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif
	memset(&buffxstat, 0, sizeof(glusterfs_fsal_xstat_t));

	/* sanity checks.
	 * note : object_attributes is optional.
	 */
	if (FSAL_TEST_MASK(attrs->mask, ATTR_SIZE)) {
		rc = glfs_h_truncate(glfs_export->gl_fs, objhandle->glhandle,
				     attrs->filesize);
		if (rc != 0) {
			status = gluster2fsal_error(errno);
			goto out;
		}
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_MODE)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_MODE);
		buffxstat.buffstat.st_mode = fsal2unix_mode(attrs->mode);
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_OWNER)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_UID);
		buffxstat.buffstat.st_uid = attrs->owner;
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_GROUP)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_GID);
		buffxstat.buffstat.st_gid = attrs->group;
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_ATIME)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_ATIME);
		buffxstat.buffstat.st_atim = attrs->atime;
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_ATIME_SERVER)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_ATIME);
		struct timespec timestamp;

		rc = clock_gettime(CLOCK_REALTIME, &timestamp);
		if (rc != 0) {
			status = gluster2fsal_error(errno);
			goto out;
		}
		buffxstat.buffstat.st_atim = timestamp;
	}

	if (FSAL_TEST_MASK(attrs->mask, ATTR_MTIME)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_MTIME);
		buffxstat.buffstat.st_mtim = attrs->mtime;
	}
	if (FSAL_TEST_MASK(attrs->mask, ATTR_MTIME_SERVER)) {
		FSAL_SET_MASK(mask, GLAPI_SET_ATTR_MTIME);
		struct timespec timestamp;

		rc = clock_gettime(CLOCK_REALTIME, &timestamp);
		if (rc != 0) {
			status = gluster2fsal_error(rc);
			goto out;
		}
		buffxstat.buffstat.st_mtim = timestamp;
	}

	/* TODO: Check for attributes not supported and return */
	/* EATTRNOTSUPP error.  */

	if (NFSv4_ACL_SUPPORT) {
		if (FSAL_TEST_MASK(attrs->mask, ATTR_ACL)) {
			if (obj_hdl->type == DIRECTORY)
				buffxstat.is_dir = true;
			else
				buffxstat.is_dir = false;

			FSAL_SET_MASK(attr_valid, XATTR_ACL);
			status =
			  glusterfs_process_acl(glfs_export->gl_fs,
						objhandle->glhandle,
						attrs, &buffxstat);

			if (FSAL_IS_ERROR(status))
				goto out;
			/* setting the ACL will set the */
			/* mode-bits too if not already passed */
			FSAL_SET_MASK(mask, GLAPI_SET_ATTR_MODE);
		}
	} else if (FSAL_TEST_MASK(attrs->mask, ATTR_ACL)) {
		status = fsalstat(ERR_FSAL_ATTRNOTSUPP, 0);
		goto out;
	}

	/* If any stat changed, indicate that */
	if (mask != 0)
		FSAL_SET_MASK(attr_valid, XATTR_STAT);
	if (FSAL_TEST_MASK(attr_valid, XATTR_STAT)) {
		/*Only if there is any change in attrs send them down to fs */
		rc = glfs_h_setattrs(glfs_export->gl_fs,
				     objhandle->glhandle,
				     &buffxstat.buffstat,
				     mask);
		if (rc != 0) {
			status = gluster2fsal_error(errno);
			goto out;
		}
	}

	if (FSAL_TEST_MASK(attr_valid, XATTR_ACL))
		status = glusterfs_set_acl(glfs_export, objhandle, &buffxstat);

out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_setattrs);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation link
 */

static fsal_status_t linkfile(struct fsal_obj_handle *obj_hdl,
			      struct fsal_obj_handle *destdir_hdl,
			      const char *name)
{
	int rc = 0, credrc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
	struct glusterfs_handle *dstparenthandle =
	    container_of(destdir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	credrc =
	    setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			    &op_ctx->creds->caller_gid,
			    op_ctx->creds->caller_glen,
			    op_ctx->creds->caller_garray);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	rc = glfs_h_link(glfs_export->gl_fs, objhandle->glhandle,
			 dstparenthandle->glhandle, name);

	credrc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (rc != 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_linkfile);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation rename
 */

static fsal_status_t renamefile(struct fsal_obj_handle *obj_hdl,
				struct fsal_obj_handle *olddir_hdl,
				const char *old_name,
				struct fsal_obj_handle *newdir_hdl,
				const char *new_name)
{
	int rc = 0, credrc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export,
			 export);
	struct glusterfs_handle *srcparenthandle =
	    container_of(olddir_hdl, struct glusterfs_handle, handle);
	struct glusterfs_handle *dstparenthandle =
	    container_of(newdir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	credrc =
	    setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			    &op_ctx->creds->caller_gid,
			    op_ctx->creds->caller_glen,
			    op_ctx->creds->caller_garray);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	rc = glfs_h_rename(glfs_export->gl_fs, srcparenthandle->glhandle,
			   old_name, dstparenthandle->glhandle, new_name);

	credrc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (rc != 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_renamefile);
#endif

	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation unlink
 */

static fsal_status_t file_unlink(struct fsal_obj_handle *dir_hdl,
				 const char *name)
{
	int rc = 0, credrc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *parenthandle =
	    container_of(dir_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	credrc =
	    setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			    &op_ctx->creds->caller_gid,
			    op_ctx->creds->caller_glen,
			    op_ctx->creds->caller_garray);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	rc = glfs_h_unlink(glfs_export->gl_fs, parenthandle->glhandle, name);

	credrc = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (credrc != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	if (rc != 0)
		status = gluster2fsal_error(errno);

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_unlink);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation open
 */

static fsal_status_t file_open(struct fsal_obj_handle *obj_hdl,
			       fsal_openflags_t openflags)
{
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glfs_fd *glfd = NULL;
	int p_flags = 0;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (objhandle->globalfd.openflags != FSAL_O_CLOSED)
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);

	fsal2posix_openflags(openflags, &p_flags);

	glfd = glfs_h_open(glfs_export->gl_fs, objhandle->glhandle, p_flags);
	if (glfd == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	objhandle->globalfd.openflags = openflags;
	objhandle->globalfd.glfd = glfd;

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_open);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation status
 */

static fsal_openflags_t file_status(struct fsal_obj_handle *obj_hdl)
{
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);

	return objhandle->globalfd.openflags;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation read
 */

static fsal_status_t file_read(struct fsal_obj_handle *obj_hdl,
			       uint64_t seek_descriptor, size_t buffer_size,
			       void *buffer, size_t *read_amount,
			       bool *end_of_file)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	rc = glfs_pread(objhandle->globalfd.glfd, buffer, buffer_size, seek_descriptor,
			0 /*TODO: flags is unused, so pass in something */);
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	if (rc < buffer_size)
		*end_of_file = true;

	*read_amount = rc;

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_read);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation write
 */

static fsal_status_t file_write(struct fsal_obj_handle *obj_hdl,
				uint64_t seek_descriptor, size_t buffer_size,
				void *buffer, size_t *write_amount,
				bool *fsal_stable)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	rc = glfs_pwrite(objhandle->globalfd.glfd, buffer, buffer_size, seek_descriptor,
			 ((*fsal_stable) ? O_SYNC : 0));
	if (rc < 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	*write_amount = rc;
	if (objhandle->globalfd.openflags & FSAL_O_SYNC)
		*fsal_stable = true;

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_write);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation commit
 *
 * This function commits the entire file and ignores the range provided
 */

static fsal_status_t commit(struct fsal_obj_handle *obj_hdl,	/* sync */
			    off_t offset, size_t len)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	/* TODO: Everybody pretty much ignores the range sent */
	rc = glfs_fsync(objhandle->globalfd.glfd);
	if (rc < 0)
		status = gluster2fsal_error(errno);
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_commit);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation lock_op
 *
 * The lock operations do not yet support blocking locks, as cancel is probably
 * needed and the current implementation would block a thread that seems
 * excessive.
 */

static fsal_status_t lock_op(struct fsal_obj_handle *obj_hdl,
			     void *p_owner,
			     fsal_lock_op_t lock_op,
			     fsal_lock_param_t *request_lock,
			     fsal_lock_param_t *conflicting_lock)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
	struct flock flock;
	int cmd;
	int saverrno = 0;
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (objhandle->globalfd.openflags == FSAL_O_CLOSED) {
		LogDebug(COMPONENT_FSAL,
			 "ERROR: Attempting to lock with no file descriptor open");
		status.major = ERR_FSAL_FAULT;
		goto out;
	}

	if (lock_op == FSAL_OP_LOCKT) {
		cmd = F_GETLK;
	} else if (lock_op == FSAL_OP_LOCK || lock_op == FSAL_OP_UNLOCK) {
		cmd = F_SETLK;
	} else {
		LogDebug(COMPONENT_FSAL,
			 "ERROR: Unsupported lock operation %d\n", lock_op);
		status.major = ERR_FSAL_NOTSUPP;
		goto out;
	}

	if (request_lock->lock_type == FSAL_LOCK_R) {
		flock.l_type = F_RDLCK;
	} else if (request_lock->lock_type == FSAL_LOCK_W) {
		flock.l_type = F_WRLCK;
	} else {
		LogDebug(COMPONENT_FSAL,
			 "ERROR: The requested lock type was not read or write.");
		status.major = ERR_FSAL_NOTSUPP;
		goto out;
	}

	/* TODO: Override R/W and just provide U? */
	if (lock_op == FSAL_OP_UNLOCK)
		flock.l_type = F_UNLCK;

	flock.l_len = request_lock->lock_length;
	flock.l_start = request_lock->lock_start;
	flock.l_whence = SEEK_SET;

	/* flock.l_len being signed long integer, larger lock ranges may
	 * get mapped to negative values. As per 'man 3 fcntl', posix
	 * locks can accept negative l_len values which may lead to
	 * unlocking an unintended range. Better bail out to prevent that.
	 *
	 * TODO: How do we support larger ranges (>INT64_MAX) then?
	 */
	if (flock.l_len < 0) {
		LogCrit(COMPONENT_FSAL,
			"The requested lock length is out of range- flock.l_len(%"
			PRIi64 "), request_lock_length(%" PRIu64 ")",
			flock.l_len, request_lock->lock_length);
		status.major = ERR_FSAL_BAD_RANGE;
		goto out;
	}

	rc = glfs_posix_lock(objhandle->globalfd.glfd, cmd, &flock);
	if (rc != 0 && lock_op == FSAL_OP_LOCK
	    && conflicting_lock && (errno == EACCES || errno == EAGAIN)) {
		/* process conflicting lock */
		saverrno = errno;
		cmd = F_GETLK;
		rc = glfs_posix_lock(objhandle->globalfd.glfd, cmd, &flock);
		if (rc) {
			LogCrit(COMPONENT_FSAL,
				"Failed to get conflicting lock post lock failure");
			status = gluster2fsal_error(errno);
			goto out;
		}

		conflicting_lock->lock_length = flock.l_len;
		conflicting_lock->lock_start = flock.l_start;
		conflicting_lock->lock_type = flock.l_type;

		status = gluster2fsal_error(saverrno);
		goto out;
	} else if (rc != 0) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	if (conflicting_lock != NULL) {
		if (lock_op == FSAL_OP_LOCKT && flock.l_type != F_UNLCK) {
			conflicting_lock->lock_length = flock.l_len;
			conflicting_lock->lock_start = flock.l_start;
			conflicting_lock->lock_type = flock.l_type;
		} else {
			conflicting_lock->lock_length = 0;
			conflicting_lock->lock_start = 0;
			conflicting_lock->lock_type = FSAL_NO_LOCK;
		}
	}

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_lock_op);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation share_op
 */
/*
static fsal_status_t share_op(struct fsal_obj_handle *obj_hdl,
			      void *p_owner,
			      fsal_share_param_t  request_share)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation close
 */

static fsal_status_t file_close(struct fsal_obj_handle *obj_hdl)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (!objhandle->globalfd.openflags ||
	    (objhandle->globalfd.openflags & FSAL_O_CLOSED)) {
		return status;
	}

	rc = glfs_close(objhandle->globalfd.glfd);
	if (rc != 0) {
		status = gluster2fsal_error(errno);
		LogCrit(COMPONENT_FSAL,
			"Error : close returns with %s", strerror(errno));
	}

	objhandle->globalfd.glfd = NULL;
	objhandle->globalfd.openflags = FSAL_O_CLOSED;

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_close);
#endif
	return status;
}

fsal_status_t glusterfs_open_my_fd(struct glusterfs_handle *objhandle,
				   fsal_openflags_t openflags,
				   int posix_flags,
				   struct glusterfs_fd *my_fd)
{
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glfs_fd *glfd = NULL;
	int p_flags = 0;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	LogFullDebug(COMPONENT_FSAL,
		     "my_fd->fd = %p openflags = %x, posix_flags = %x",
		     my_fd->glfd, openflags, posix_flags);

	assert(my_fd->glfd == NULL
	       && my_fd->openflags == FSAL_O_CLOSED && openflags != 0);

	fsal2posix_openflags(openflags, &p_flags);

	LogFullDebug(COMPONENT_FSAL,
		     "openflags = %x, posix_flags = %x",
		     openflags, posix_flags);

	glfd = glfs_h_open(glfs_export->gl_fs, objhandle->glhandle, p_flags);
	if (glfd == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	my_fd->glfd = glfd;
	my_fd->openflags = openflags;

out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_open);
#endif
	return status;
}

fsal_status_t glusterfs_close_my_fd(struct glusterfs_fd *my_fd)
{
	int rc = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };

#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (my_fd->glfd && my_fd->openflags != FSAL_O_CLOSED) {
		rc = glfs_close(my_fd->glfd);
		if (rc != 0) {
			status = gluster2fsal_error(errno);
			LogCrit(COMPONENT_FSAL,
				"Error : close returns with %s",
				strerror(errno));
		}
	}

	my_fd->glfd = NULL;
	my_fd->openflags = FSAL_O_CLOSED;

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_close);
#endif
	return status;
}

static inline bool not_open_correct(struct glusterfs_fd *my_fd,
				    fsal_openflags_t openflags)
{
	/* 1. my_fd->openflags will NEVER be FSAL_O_ANY.
	 * 2. If openflags == FSAL_O_ANY, the first half will be true if the
	 *    file is closed, and the second half MUST be true (per statement 1)
	 * 3. If openflags is anything else, the first half will be true and
	 *    the second half will be true if my_fd->openflags does not include
	 *    the requested modes.
	 */
	return (openflags != FSAL_O_ANY || my_fd->openflags == FSAL_O_CLOSED)
	       && ((my_fd->openflags & openflags) != openflags);
}


static inline bool open_correct(struct glusterfs_fd *my_fd,
				fsal_openflags_t openflags)
{
	return (openflags == FSAL_O_ANY && my_fd->openflags != FSAL_O_CLOSED)
	       || (openflags != FSAL_O_ANY
		   && (my_fd->openflags & openflags & FSAL_O_RDWR)
						== (openflags & FSAL_O_RDWR));
}

/**
 * @brief Reopen the fd associated with the object handle.
 *
 * This function assures that the fd is open in the mode requested. If
 * the fd was already open, it closes it and reopens with the OR of the
 * requested modes.
 *
 * This function will return with the object handle lock held for read
 * if successful.
 *
 * @param[in]  myself      File on which to operate
 * @param[in]  check_share Indicates we must check for share conflict
 * @param[in]  openflags   Mode for open
 * @param[out] fd          File descriptor that is to be used
 * @param[out] has_lock    Indicates that obj_hdl->lock is held read
 * @param[out] closefd     Indicates that file descriptor must be closed
 *
 * @return FSAL status.
 */

fsal_status_t glusterfs_reopen_obj(struct fsal_obj_handle *obj_hdl,
			     bool check_share,
			     bool bypass,
			     fsal_openflags_t openflags,
			     struct glusterfs_fd *my_fd_arg,
			     bool *has_lock,
			     bool *closefd)
{
	struct glusterfs_handle *myself;
	int posix_flags = 0;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	int rc;
	bool retried = false;
	fsal_openflags_t try_openflags;
	struct glusterfs_fd *my_fd = NULL;

	/* Use the global file descriptor. */
	myself = container_of(obj_hdl, struct glusterfs_handle, handle);
	my_fd = &myself->globalfd;
	*closefd = false;

	/* Take read lock on object to protect file descriptor.
	 * We only take a read lock because we are not changing the
	 * state of the file descriptor.
	 */
	PTHREAD_RWLOCK_rdlock(&obj_hdl->lock);

	if (check_share) {
		/* Note we will check again if we drop and re-acquire the lock
		 * just to be on the safe side.
		 */
		status = check_share_conflict(&myself->share,
					      openflags,
					      bypass);

		if (FSAL_IS_ERROR(status)) {
			PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
			*has_lock = false;
			return status;
		}
	}

again:

	LogFullDebug(COMPONENT_FSAL,
		     "Open mode = %x, desired mode = %x",
		     (int) my_fd->openflags,
		     (int) openflags);

	if (not_open_correct(my_fd, openflags)) {

		/* Drop the rwlock */
		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);

		if (retried) {
			/* This really should never occur, it could occur
			 * if there was some race with closing the file.
			 */
			LogDebug(COMPONENT_FSAL,
				 "Retry failed, returning EBADF");
			*has_lock = false;
			return fsalstat(posix2fsal_error(EBADF), EBADF);
		}

		/* Switch to write lock on object to protect file descriptor.
		 * By using trylock, we don't block if another thread is using
		 * the file descriptor right now. In that case, we just open
		 * a temporary file descriptor.
		 *
		 * This prevents us from blocking for the duration of an
		 * I/O request.
		 */
		rc = pthread_rwlock_trywrlock(&obj_hdl->lock);
		if (rc == EBUSY) {
			/* Someone else is using the file descriptor.
			 * Just provide a temporary file descriptor.
			 * We still take a read lock so we can protect the
			 * share reservation for the duration of the caller's
			 * operation if we needed to check.
			 */
			if (check_share) {
				PTHREAD_RWLOCK_rdlock(&obj_hdl->lock);

				status =
				    check_share_conflict(&myself->share,
							 openflags,
							 bypass);

				if (FSAL_IS_ERROR(status)) {
					PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
					*has_lock = false;
					return status;
				}
			}

			fsal2posix_openflags(openflags, &posix_flags);

			status = glusterfs_open_my_fd(myself, openflags,
						      posix_flags, my_fd);

			if (FSAL_IS_ERROR(status)) {
				LogDebug(COMPONENT_FSAL,
					 "glusterfs_open failed with %s openflags 0x%08x",
					 strerror(errno), openflags);
				*has_lock = false;
				return status;
			}

			*closefd = true;
			*has_lock = check_share;
			return fsalstat(ERR_FSAL_NO_ERROR, 0);

		} else if (rc != 0) {
			LogCrit(COMPONENT_RW_LOCK,
				"Error %d, read locking %p", rc, myself);
			abort();
		}

		if (check_share) {
			status = check_share_conflict(&myself->share,
						      openflags,
						      bypass);

			if (FSAL_IS_ERROR(status)) {
				PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
				*has_lock = false;
				return status;
			}
		}

		LogFullDebug(COMPONENT_FSAL,
			     "Open mode = %x, desired mode = %x",
			     (int) my_fd->openflags,
			     (int) openflags);

		if (not_open_correct(my_fd, openflags)) {
			if (my_fd->openflags != FSAL_O_CLOSED) {
				/* Add desired mode to existing mode. */
				try_openflags = openflags | my_fd->openflags;

				/* Now close the already open descriptor. */
				status = glusterfs_close_my_fd(my_fd);

				if (FSAL_IS_ERROR(status)) {
					PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
					LogDebug(COMPONENT_FSAL,
						 "glusterfs_close_my_fd failed with %s",
						 strerror(status.minor));
					*has_lock = false;
					return status;
				}
			} else if (openflags == FSAL_O_ANY) {
				try_openflags = FSAL_O_READ;
			} else {
				try_openflags = openflags;
			}

			fsal2posix_openflags(try_openflags, &posix_flags);

			LogFullDebug(COMPONENT_FSAL,
				     "try_openflags = %x, posix_flags = %x",
				     try_openflags, posix_flags);

			/* Actually open the file */
			status = glusterfs_open_my_fd(myself, try_openflags,
						      posix_flags, my_fd);

			if (FSAL_IS_ERROR(status)) {
				PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
				LogDebug(COMPONENT_FSAL,
					 "glusterfs_open_my_fd failed with %s",
					 strerror(status.minor));
				*has_lock = false;
				return status;
			}
		}

		/* Ok, now we should be in the correct mode.
		 * Switch back to read lock and try again.
		 * We don't want to hold the write lock because that would
		 * block other users of the file descriptor.
		 * Since we dropped the lock, we need to verify mode is still'
		 * good after we re-aquire the read lock, thus the retry.
		 */
		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
		PTHREAD_RWLOCK_rdlock(&obj_hdl->lock);
		retried = true;

		if (check_share) {
			status = check_share_conflict(&myself->share,
						      openflags,
						      bypass);

			if (FSAL_IS_ERROR(status)) {
				PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
				*has_lock = false;
				return status;
			}
		}
		goto again;
	}

	*has_lock = true;
	my_fd_arg->glfd = my_fd->glfd;
	my_fd_arg->openflags = my_fd->openflags;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t find_fd(struct glusterfs_fd *my_fd,
		      struct fsal_obj_handle *obj_hdl,
		      bool bypass,
		      struct state_t *state,
		      fsal_openflags_t openflags,
		      bool *has_lock,
		      bool *need_fsync,
		      bool *closefd,
		      bool open_for_locks)
{
	struct glusterfs_handle *myself;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	int posix_flags;
	struct glusterfs_fd  tmp_fd = {0}, *tmp2_fd;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);

	myself = container_of(obj_hdl, struct glusterfs_handle, handle);

	/* Handle non-regular files */
	switch (obj_hdl->type) {
	case SOCKET_FILE:
	case CHARACTER_FILE:
	case BLOCK_FILE:
		/* XXX: check for O_NOACCESS. Refer vfs find_fd */
		posix_flags = O_PATH;
		goto regular_open;

	case REGULAR_FILE:
		/* Handle below */
		break;

	case SYMBOLIC_LINK:
		posix_flags |= (O_PATH | O_RDWR | O_NOFOLLOW);
		goto regular_open;

	case FIFO_FILE:
		posix_flags |= O_NONBLOCK;
		/* fall through */

 regular_open:
		status = glusterfs_open_my_fd(myself,
					       openflags,
					       posix_flags,
					       &tmp_fd);
		if (FSAL_IS_ERROR(status)) {
			LogCrit(COMPONENT_FSAL,
				 "Failed with %s openflags 0x%08x",
				 strerror(errno), openflags);
			return fsalstat(posix2fsal_error(errno), errno);
		}

		my_fd->glfd = tmp_fd.glfd;
		my_fd->openflags = tmp_fd.openflags;

		LogFullDebug(COMPONENT_FSAL,
			     "Opened glfd=%p for file of type %s",
			     my_fd->glfd,
			     object_file_type_to_str(obj_hdl->type));

		*closefd = true;
		return status;
	case DIRECTORY:
		my_fd->glfd = glfs_h_opendir(glfs_export->gl_fs,
						myself->glhandle);
		if (my_fd->glfd == NULL)
			return gluster2fsal_error(errno);
		*closefd = true;
		return status;

	case NO_FILE_TYPE:
	case EXTENDED_ATTR:
		return fsalstat(posix2fsal_error(EINVAL), EINVAL);
	}

	if (state == NULL)
		goto global;

	/* State was valid, check it's fd */
	tmp2_fd = (struct glusterfs_fd *)(state + 1);

		my_fd->glfd = tmp2_fd->glfd;
		my_fd->openflags = tmp2_fd->openflags;

	LogFullDebug(COMPONENT_FSAL,
		     "my_fd->openflags = %d openflags = %d",
		     my_fd->openflags, openflags);

	if (open_correct(my_fd, openflags)) {
		/* It was valid, return it.
		 * Since we found a valid fd in the sate, no need to
		 * check deny modes.
		 */
		LogFullDebug(COMPONENT_FSAL, "Use state fd");
		*need_fsync = (openflags & FSAL_O_SYNC) != 0;
		return status;
	}

	if (open_for_locks) {
		if (my_fd->openflags != FSAL_O_CLOSED) {
			LogCrit(COMPONENT_FSAL,
				"Conflicting open, can not re-open fd with locks");
			return fsalstat(posix2fsal_error(EINVAL), EINVAL);
		}

		/* This is being opened for locks, we will not be able to
		 * re-open so open for read/write unless openstate indicates
		 * something different.
		 */
		if (state->state_data.lock.openstate != NULL) {
			struct glusterfs_fd *related_fd =
					(struct glusterfs_fd *)
					(state->state_data.lock.openstate + 1);

			openflags = related_fd->openflags & FSAL_O_RDWR;
		} else {
			/* No associated open, open read/write. */
			openflags = FSAL_O_RDWR;
		}

		fsal2posix_openflags(openflags, &posix_flags);

		status = glusterfs_open_my_fd(myself, openflags,
					 posix_flags, &tmp_fd);

		if (FSAL_IS_ERROR(status)) {
			LogCrit(COMPONENT_FSAL,
				"Open for locking failed");
		} else {
			*need_fsync = false;
		}
		my_fd->glfd = tmp_fd.glfd;
		my_fd->openflags = tmp_fd.openflags;

		return status;
	}

	if ((state->state_type == STATE_TYPE_LOCK ||
	     state->state_type == STATE_TYPE_NLM_LOCK) &&
	    state->state_data.lock.openstate != NULL) {
		tmp2_fd = (struct glusterfs_fd *)
			  (state->state_data.lock.openstate + 1);

		my_fd->glfd = tmp2_fd->glfd;
		my_fd->openflags = tmp2_fd->openflags;

		if (open_correct(my_fd, openflags)) {
			/* It was valid, return it.
			 * Since we found a valid fd in the state, no need to
			 * check deny modes.
			 */
			LogFullDebug(COMPONENT_FSAL, "Use open state fd");
			*need_fsync = (openflags & FSAL_O_SYNC) != 0;
			return status;
		}
	}

 global:

	/* No useable state_t so return the global file descriptor. */
	LogFullDebug(COMPONENT_FSAL,
		     "Use global fd openflags = %x",
		     openflags);

	/* We will take the object handle lock in glusterfs_reopen_obj.
	 * And we won't have to fsync. XXX: what with fsync??
	 */
	*need_fsync = false;

	/* Make sure global is open as necessary otherwise return a
	 * temporary file descriptor. Check share reservation if not
	 * opening FSAL_O_ANY.
	 */
	status = glusterfs_reopen_obj(obj_hdl, openflags != FSAL_O_ANY, bypass,
			      openflags, &tmp_fd, has_lock, closefd);
		my_fd->glfd = tmp_fd.glfd;
		my_fd->openflags = tmp_fd.openflags;
	return status;

}

fsal_status_t fetch_attrs(struct glusterfs_handle *myself,
			  struct glusterfs_fd *my_fd)
{
	int retval = 0;
	fsal_status_t status = {0, 0};
	const char *func = "unknown";
	glusterfs_fsal_xstat_t buffxstat;
	struct attrlist *fsalattr;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);


	/* Now stat the file as appropriate */
	switch (myself->handle.type) {
	case SOCKET_FILE:
	case CHARACTER_FILE:
	case BLOCK_FILE:
	case SYMBOLIC_LINK: /* XXX: do we need glfd? */
	case FIFO_FILE:
		retval = glfs_h_stat(glfs_export->gl_fs, myself->glhandle,
					&buffxstat.buffstat);
		func = "stat";
		break;
	case REGULAR_FILE:
	case DIRECTORY:
		retval = glfs_fstat(my_fd->glfd, &buffxstat.buffstat);
		func = "fstat";
		break;

	case NO_FILE_TYPE:
	case EXTENDED_ATTR:
		/* Caught during open with EINVAL */
		break;
	}

	if (retval < 0) {
		if (errno == ENOENT)
			retval = ESTALE;
		else
			retval = errno;

		LogDebug(COMPONENT_FSAL, "%s failed with %s", func,
			 strerror(retval));

		status = gluster2fsal_error(retval);
		goto out;
	}

	fsalattr = &myself->attributes;
	stat2fsal_attributes(&buffxstat.buffstat, &myself->attributes);

	if (myself->handle.type == DIRECTORY)
		buffxstat.is_dir = true;
	else
		buffxstat.is_dir = false;

	status = glusterfs_get_acl(glfs_export, myself->glhandle,
				   &buffxstat, fsalattr);

	/*
	 * The error ENOENT is not an expected error for GETATTRS.
	 * Due to this, operations such as RENAME will fail when it
	 * calls GETATTRS on removed file.
	 */
	if (status.minor == ENOENT)
		status = gluster2fsal_error(ESTALE);

out:
		if (FSAL_IS_ERROR(status)) {
			FSAL_CLEAR_MASK(myself->attributes.mask);
			FSAL_SET_MASK(myself->attributes.mask, ATTR_RDATTR_ERR);
		}
	return status;
}

/* open2
 */

static fsal_status_t glusterfs_open2(struct fsal_obj_handle *obj_hdl,
			   struct state_t *state,
			   fsal_openflags_t openflags,
			   enum fsal_create_mode createmode,
			   const char *name,
			   struct attrlist *attrib_set,
			   fsal_verifier_t verifier,
			   struct fsal_obj_handle **new_obj,
			   bool *caller_perm_check)
{
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	int p_flags = 0;
	struct glusterfs_export *glfs_export =
	    container_of(op_ctx->fsal_export, struct glusterfs_export, export);
	struct glusterfs_handle *myself, *parenthandle = NULL;
	struct glusterfs_fd *my_fd = NULL, tmp_fd = {0};
	struct stat sb;
	struct glfs_object *glhandle = NULL;
	unsigned char globjhdl[GFAPI_HANDLE_LENGTH] = {'\0'};
	char vol_uuid[GLAPI_UUID_LENGTH] = {'\0'};
	bool truncated;
	bool setattrs = attrib_set != NULL;
	bool created = false;
	struct attrlist verifier_attr;
	int retval = 0;
	mode_t unix_mode;


#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (state != NULL)
		my_fd = (struct glusterfs_fd *)(state + 1);

	fsal2posix_openflags(openflags, &p_flags);

	if (createmode != FSAL_NO_CREATE && setattrs &&
	    FSAL_TEST_MASK(attrib_set->mask, ATTR_SIZE) &&
	    attrib_set->filesize == 0) {
		LogFullDebug(COMPONENT_FSAL, "Truncate");
		/* Handle truncate to zero on open */
		p_flags |= O_TRUNC;
		/* Don't set the size if we later set the attributes */
		FSAL_UNSET_MASK(attrib_set->mask, ATTR_SIZE);
	}

	truncated = (p_flags & O_TRUNC) != 0;

	/* Now fixup attrs for verifier if exclusive create */
	if (createmode >= FSAL_EXCLUSIVE) {
		if (!setattrs) {
			/* We need to use verifier_attr */
			attrib_set = &verifier_attr;
			memset(&verifier_attr, 0, sizeof(verifier_attr));
		}

		set_common_verifier(attrib_set, verifier);
	}

	if (name == NULL) {
		/* This is an open by handle */
		struct glusterfs_handle *myself;

		myself = container_of(obj_hdl,
				      struct glusterfs_handle,
				      handle);

	/* XXX: fsid work
		if (obj_hdl->fsal != obj_hdl->fs->fsal) {
			LogDebug(COMPONENT_FSAL,
				 "FSAL %s operation for handle belonging to
				 FSAL %s, return EXDEV",
				 obj_hdl->fsal->name, obj_hdl->fs->fsal->name);
			return fsalstat(posix2fsal_error(EXDEV), EXDEV);
		}*/

		if (state != NULL) {
			/* Prepare to take the share reservation, but only if we
			 * are called with a valid state (if state is NULL the
			 * caller is a stateless create such as NFS v3 CREATE).
			 */

			/* This can block over an I/O operation. */
			PTHREAD_RWLOCK_wrlock(&obj_hdl->lock);

			/* Check share reservation conflicts. */
			status = check_share_conflict(&myself->share,
						      openflags,
						      false);

			if (FSAL_IS_ERROR(status)) {
				PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
				return status;
			}

			/* Take the share reservation now by updating the
			 * counters.
			 */
			update_share_counters(&myself->share,
					      FSAL_O_CLOSED,
					      openflags);

			PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
		} else {
			/* We need to use the global fd to continue, and take
			 * the lock to protect it.
			 */
			my_fd = &myself->globalfd;
			PTHREAD_RWLOCK_wrlock(&obj_hdl->lock);
		}

		/* truncate is set in p_flags */
		status = glusterfs_open_my_fd(myself, openflags, p_flags,
					       &tmp_fd);

		if (FSAL_IS_ERROR(status)) {
			status = gluster2fsal_error(errno);
			if (state == NULL) {
				/* Release the lock taken above, and return
				 * since there is nothing to undo.
				 */
				PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
				goto out;
			} else {
				/* Error - need to release the share */
				goto undo_share;
			}
		}

		my_fd->glfd = tmp_fd.glfd;
		my_fd->openflags = tmp_fd.openflags;

		if (createmode >= FSAL_EXCLUSIVE || truncated) {
			/* Refresh the attributes to return client the
			 * attributes which got set */
			status = fetch_attrs(myself, my_fd);

			if (FSAL_IS_ERROR(status)) {
				FSAL_CLEAR_MASK(myself->attributes.mask);
				FSAL_SET_MASK(myself->attributes.mask,
					       ATTR_RDATTR_ERR);
				/** @todo: should handle this
				 * better.
				 */
			}
			LogFullDebug(COMPONENT_FSAL, "New size = %"PRIx64,
				     myself->attributes.filesize);

			/* Now check verifier for exclusive, but not for
			 * FSAL_EXCLUSIVE_9P.
			 */
			if (!FSAL_IS_ERROR(status) &&
			    createmode >= FSAL_EXCLUSIVE &&
			    createmode != FSAL_EXCLUSIVE_9P &&
			    !obj_hdl->obj_ops.check_verifier(obj_hdl,
							     verifier)) {
				/* Verifier didn't match, return EEXIST */
				status =
				    fsalstat(posix2fsal_error(EEXIST), EEXIST);
			}
		}

		if (state == NULL) {
			/* If no state, release the lock taken above and return
			 * status.
			 */
			PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
			return status;
		}

		if (!FSAL_IS_ERROR(status)) {
			/* Return success. */
			return status;
		}

		(void) glusterfs_close_my_fd(my_fd);
 undo_share:

		/* Can only get here with state not NULL and an error */

		/* On error we need to release our share reservation
		 * and undo the update of the share counters.
		 * This can block over an I/O operation.
		 */
		PTHREAD_RWLOCK_wrlock(&obj_hdl->lock);

		update_share_counters(&myself->share,
				      openflags,
				      FSAL_O_CLOSED);

		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);

		return status;
	}

/* case name_not_null */
	/* In this path where we are opening by name, we can't check share
	 * reservation yet since we don't have an object_handle yet. If we
	 * indeed create the object handle (there is no race with another
	 * open by name), then there CAN NOT be a share conflict, otherwise
	 * the share conflict will be resolved when the object handles are
	 * merged.
	 */

	/* Now add in O_CREAT and O_EXCL.
	 * Even with FSAL_UNGUARDED we try exclusive create first so
	 * we can safely set attributes.
	 */
	if (createmode != FSAL_NO_CREATE) {
		p_flags |= O_CREAT;

		if (createmode >= FSAL_GUARDED || setattrs)
			p_flags |= O_EXCL;
	}

	if (setattrs && FSAL_TEST_MASK(attrib_set->mask, ATTR_MODE)) {
		unix_mode = fsal2unix_mode(attrib_set->mode) &
		    ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);
		/* Don't set the mode if we later set the attributes */
		FSAL_UNSET_MASK(attrib_set->mask, ATTR_MODE);
	} else {
		/* Default to mode 0600 */
		unix_mode = 0600;
	}

	/* XXX: we do not have openat implemented yet..meanwhile
	 *  use 'glfs_h_creat' */

	/* obtain parent directory handle */
	parenthandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);

	/* Become the user because we are creating an object in this dir.
	 */
	if (createmode != FSAL_NO_CREATE) {

		/* set proper credentials */
		retval = setglustercreds(glfs_export,
					&op_ctx->creds->caller_uid,
					&op_ctx->creds->caller_gid,
					op_ctx->creds->caller_glen,
					op_ctx->creds->caller_garray);

		if (retval != 0) {
			status = gluster2fsal_error(EPERM);
			LogFatal(COMPONENT_FSAL,
				 "Could not set Ganesha credentials");
			goto out;
		}
	}

	/* XXX: Not sure if glfs_h_creat honours NO_CREATE mode */
	glhandle =
	    glfs_h_creat(glfs_export->gl_fs, parenthandle->glhandle, name,
			 p_flags, unix_mode, &sb);

	if (glhandle == NULL && errno == EEXIST &&
		 createmode == FSAL_UNCHECKED) {
		/* We tried to create O_EXCL to set attributes and failed.
		 * Remove O_EXCL and retry, also remember not to set attributes.
		 * We still try O_CREAT again just in case file disappears out
		 * from under us.
		 */
		p_flags &= ~O_EXCL;
		setattrs = false;
		glhandle =
		    glfs_h_creat(glfs_export->gl_fs, parenthandle->glhandle,
				 name, p_flags, unix_mode, &sb);
	}

       /* preserve errno */
	retval = errno;

	/* restore credentials */
	if (createmode != FSAL_NO_CREATE) {
		retval = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
		if (retval != 0) {
			status = gluster2fsal_error(EPERM);
			LogFatal(COMPONENT_FSAL,
				 "Could not set Ganesha credentials");
			goto out;
		}
	}

	if (glhandle == NULL) {
		status = gluster2fsal_error(errno);
		goto out;
	}

	/* Remember if we were responsible for creating the file.
	 * Note that in an UNCHECKED retry we MIGHT have re-created the
	 * file and won't remember that. Oh well, so in that rare case we
	 * leak a partially created file if we have a subsequent error in here.
	 * Also notify caller to do permission check if we DID NOT create the
	 * file. Note it IS possible in the case of a race between an UNCHECKED
	 * open and an external unlink, we did create the file, but we will
	 * still force a permission check. Of course that permission check
	 * SHOULD succeed since we also won't set the mode the caller requested
	 * and the default file create permissions SHOULD allow the owner
	 * read/write.
	 */
	created = (p_flags & O_EXCL) != 0;
	*caller_perm_check = !created;

	retval = glfs_h_extract_handle(glhandle, globjhdl, GFAPI_HANDLE_LENGTH);
	if (retval < 0) {
		status = gluster2fsal_error(errno);
		goto direrr;
	}

	retval = glfs_get_volumeid(glfs_export->gl_fs, vol_uuid,
				   GLAPI_UUID_LENGTH);
	if (retval < 0) {
		status = gluster2fsal_error(retval);
		goto direrr;
	}

	construct_handle(glfs_export, &sb, glhandle, globjhdl,
			 GLAPI_HANDLE_LENGTH, &myself, vol_uuid);

	/* If we didn't have a state above, use the global fd. At this point,
	 * since we just created the global fd, no one else can have a
	 * reference to it, and thus we can mamnipulate unlocked which is
	 * handy since we can then call setattr2 which WILL take the lock
	 * without a double locking deadlock.
	 */
	if (my_fd == NULL)
		my_fd = &myself->globalfd;

	/* now open it */
	status = glusterfs_open_my_fd(myself, openflags, p_flags, my_fd);

	if (FSAL_IS_ERROR(status))
		goto direrr;

	*new_obj = &myself->handle;

	if (setattrs && attrib_set->mask != 0) {
		/* Set attributes using our newly opened file descriptor as the
		 * share_fd if there are any left to set (mode and truncate
		 * have already been handled).
		 *
		 * Note that we only set the attributes if we were responsible
		 * for creating the file.
		 */
		status = (*new_obj)->obj_ops.setattr2(*new_obj,
						      false,
						      state,
						      attrib_set);

		if (FSAL_IS_ERROR(status)) {
			/* Release the handle we just allocated. */
			(*new_obj)->obj_ops.release(*new_obj);
			*new_obj = NULL;
			goto fileerr;
		}
	}


	if (state != NULL) {
		/* Prepare to take the share reservation, but only if we are
		 * called with a valid state (if state is NULL the caller is
		 * a stateless create such as NFS v3 CREATE).
		 */

		/* This can block over an I/O operation. */
		PTHREAD_RWLOCK_wrlock(&(*new_obj)->lock);

		/* Take the share reservation now by updating the counters. */
		update_share_counters(&myself->share,
				      FSAL_O_CLOSED,
				      openflags);

		PTHREAD_RWLOCK_unlock(&(*new_obj)->lock);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);


fileerr:
	glusterfs_close_my_fd(my_fd);

direrr:
	/* Delete the file if we actually created it. */
	if (created)
		glfs_h_unlink(glfs_export->gl_fs, parenthandle->glhandle, name);


	if (status.major != ERR_FSAL_NO_ERROR)
		gluster_cleanup_vars(glhandle);
	return fsalstat(posix2fsal_error(retval), retval);

 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_file_open);
#endif
	return status;
}

/* status2
 * return openflags of corresponding fd
 */

static fsal_openflags_t glusterfs_status2(struct state_t *state)
{
	struct glusterfs_fd *my_fd = (struct glusterfs_fd *)(state + 1);

	return my_fd->openflags;
}

/* reopen2
 */

static fsal_status_t glusterfs_reopen2(struct fsal_obj_handle *obj_hdl,
			     struct state_t *state,
			     fsal_openflags_t openflags)
{
	struct glusterfs_fd fd = {0}, *my_fd = &fd, *my_share_fd = NULL;
	struct glusterfs_handle *myself;
	fsal_status_t status = {0, 0};
	int posix_flags = 0;
	fsal_openflags_t old_openflags;
	bool truncated;

	my_share_fd = (struct glusterfs_fd *)(state + 1);

	fsal2posix_openflags(openflags, &posix_flags);

	truncated = (posix_flags & O_TRUNC) != 0;

	memset(my_fd, 0, sizeof(*my_fd));

	myself  = container_of(obj_hdl,
			       struct glusterfs_handle,
			       handle);

	/* XXX: fsid work
	if (obj_hdl->fsal != obj_hdl->fs->fsal) {
		LogDebug(COMPONENT_FSAL,
			 "FSAL %s operation for handle belonging to FSAL %s,
			 return EXDEV",
			 obj_hdl->fsal->name, obj_hdl->fs->fsal->name);
		return fsalstat(posix2fsal_error(EXDEV), EXDEV);
	}*/

	/* This can block over an I/O operation. */
	PTHREAD_RWLOCK_wrlock(&obj_hdl->lock);

	old_openflags = my_share_fd->openflags;

	/* We can conflict with old share, so go ahead and check now. */
	status = check_share_conflict(&myself->share, openflags, false);

	if (FSAL_IS_ERROR(status)) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
		return status;
	}

	/* Set up the new share so we can drop the lock and not have a
	 * conflicting share be asserted, updating the share counters.
	 */
	update_share_counters(&myself->share, old_openflags, openflags);

	PTHREAD_RWLOCK_unlock(&obj_hdl->lock);

	status = glusterfs_open_my_fd(myself, openflags, posix_flags, my_fd);

	if (!FSAL_IS_ERROR(status)) {
		/* Close the existing file descriptor and copy the new
		 * one over.
		 */
		glusterfs_close_my_fd(my_share_fd);
		*my_share_fd = fd;

		if (truncated) {
			/* Refresh the attributes */
			status = fetch_attrs(myself, my_share_fd);
			if (FSAL_IS_ERROR(status)) {
				FSAL_CLEAR_MASK(myself->attributes.mask);
				FSAL_SET_MASK(myself->attributes.mask,
					       ATTR_RDATTR_ERR);
				 /** @todo: should handle this
				   * better.
				   */
			}
		}
	} else {
		/* We had a failure on open - we need to revert the share.
		 * This can block over an I/O operation.
		 */
		PTHREAD_RWLOCK_wrlock(&obj_hdl->lock);

		update_share_counters(&myself->share,
				      openflags,
				      old_openflags);

		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);
	}

	return status;
}

/* read2
 */

static fsal_status_t glusterfs_read2(struct fsal_obj_handle *obj_hdl,
			   bool bypass,
			   struct state_t *state,
			   uint64_t seek_descriptor,
			   size_t buffer_size,
			   void *buffer, size_t *read_amount,
			   bool *end_of_file,
			   struct io_info *info)
{
	struct glusterfs_fd my_fd = {0};
	ssize_t nb_read;
	fsal_status_t status;
	int retval = 0;
	bool has_lock = false;
	bool need_fsync = false;
	bool closefd = false;

	if (info != NULL) {
		/* Currently we don't support READ_PLUS */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	/* XXX: fsid work
	if (obj_hdl->fsal != obj_hdl->fs->fsal) {
		LogDebug(COMPONENT_FSAL,
			 "FSAL %s operation for handle belonging to FSAL %s,
			 return EXDEV",
			 obj_hdl->fsal->name, obj_hdl->fs->fsal->name);
		return fsalstat(posix2fsal_error(EXDEV), EXDEV);
	}*/

	/* Get a usable file descriptor */
	status = find_fd(&my_fd, obj_hdl, bypass, state, FSAL_O_READ,
			 &has_lock, &need_fsync, &closefd, false);

	if (FSAL_IS_ERROR(status))
		goto out;

	nb_read = glfs_pread(my_fd.glfd, buffer, buffer_size,
			     seek_descriptor, 0);

	if (seek_descriptor == -1 || nb_read == -1) {
		retval = errno;
		status = fsalstat(posix2fsal_error(retval), retval);
		goto out;
	}

	*read_amount = nb_read;

	if (nb_read < buffer_size)
		*end_of_file = true;
#if 0
	/** @todo
	 *
	 * Is this all we really need to do to support READ_PLUS? Will anyone
	 * ever get upset that we don't return holes, even for blocks of all
	 * zeroes?
	 *
	 */
	if (info != NULL) {
		info->io_content.what = NFS4_CONTENT_DATA;
		info->io_content.data.d_offset = offset + nb_read;
		info->io_content.data.d_data.data_len = nb_read;
		info->io_content.data.d_data.data_val = buffer;
	}
#endif

 out:

	if (closefd)
		glusterfs_close_my_fd(&my_fd);

	if (has_lock)
		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);

	return status;

}

/* write2
 */

static fsal_status_t glusterfs_write2(struct fsal_obj_handle *obj_hdl,
			    bool bypass,
			    struct state_t *state,
			    uint64_t seek_descriptor,
			    size_t buffer_size,
			    void *buffer,
			    size_t *write_amount,
			    bool *fsal_stable,
			    struct io_info *info)
{
	ssize_t nb_written;
	fsal_status_t status;
	int retval = 0;
	struct glusterfs_fd my_fd = {0};
	bool has_lock = false;
	bool need_fsync = false;
	bool closefd = false;
	fsal_openflags_t openflags = FSAL_O_WRITE;
	struct glusterfs_export *glfs_export =
	     container_of(op_ctx->fsal_export, struct glusterfs_export, export);


	if (info != NULL) {
		/* Currently we don't support WRITE_PLUS */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	/* XXX: fsid work
	if (obj_hdl->fsal != obj_hdl->fs->fsal) {
		LogDebug(COMPONENT_FSAL,
			 "FSAL %s operation for handle belonging to FSAL %s,
			 return EXDEV",
			 obj_hdl->fsal->name, obj_hdl->fs->fsal->name);
		return fsalstat(posix2fsal_error(EXDEV), EXDEV);
	}*/

	if (*fsal_stable)
		openflags |= FSAL_O_SYNC;

	/* Get a usable file descriptor */
	status = find_fd(&my_fd, obj_hdl, bypass, state, openflags,
			 &has_lock, &need_fsync, &closefd, false);

	if (FSAL_IS_ERROR(status))
		goto out;

	retval = setglustercreds(glfs_export, &op_ctx->creds->caller_uid,
			&op_ctx->creds->caller_gid,
			op_ctx->creds->caller_glen,
			op_ctx->creds->caller_garray);
	if (retval != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	nb_written = glfs_pwrite(my_fd.glfd, buffer, buffer_size,
			     seek_descriptor, ((*fsal_stable) ? O_SYNC : 0));

	if (nb_written == -1) {
		retval = errno;
		status = fsalstat(posix2fsal_error(retval), retval);
		goto out;
	}

	*write_amount = nb_written;

	/* restore credentials */
	retval = setglustercreds(glfs_export, NULL, NULL, 0, NULL);
	if (retval != 0) {
		status = gluster2fsal_error(EPERM);
		LogFatal(COMPONENT_FSAL, "Could not set Ganesha credentials");
		goto out;
	}

	/* attempt stability if we aren't using an O_SYNC fd */
	if (need_fsync) {
		retval = glfs_fsync(my_fd.glfd);
		if (retval == -1) {
			retval = errno;
			status = fsalstat(posix2fsal_error(retval), retval);
		}
	}

 out:

	if (closefd)
		glusterfs_close_my_fd(&my_fd);

	if (has_lock)
		PTHREAD_RWLOCK_unlock(&obj_hdl->lock);

	return status;
}

/* commit2
 * default case not supported
 */

static fsal_status_t glusterfs_commit2(struct fsal_obj_handle *obj_hdl,
			     off_t offset,
			     size_t len)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}

/* lock_op2
 * default case not supported
 */

static fsal_status_t glusterfs_lock_op2(struct fsal_obj_handle *obj_hdl,
			      struct state_t *state,
			      void *p_owner,
			      fsal_lock_op_t lock_op,
			      fsal_lock_param_t *request_lock,
			      fsal_lock_param_t *conflicting_lock)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}

/* setattr2
 * default case not supported
 */

static fsal_status_t glusterfs_setattr2(struct fsal_obj_handle *obj_hdl,
			      bool bypass,
			      struct state_t *state,
			      struct attrlist *attrs)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}

/* close2
 * default case not supported
 */

static fsal_status_t glusterfs_close2(struct fsal_obj_handle *obj_hdl,
			    struct state_t *fd)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}

/**
 * @brief Implements GLUSTER FSAL objectoperation list_ext_attrs
 */
/*
static fsal_status_t list_ext_attrs(struct fsal_obj_handle *obj_hdl,
				    const struct req_op_context *opctx,
				    unsigned int cookie,
				    fsal_xattrent_t * xattrs_tab,
				    unsigned int xattrs_tabsize,
				    unsigned int *p_nb_returned,
				    int *end_of_list)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation getextattr_id_by_name
 */
/*
static fsal_status_t getextattr_id_by_name(struct fsal_obj_handle *obj_hdl,
					   const struct req_op_context *opctx,
					   const char *xattr_name,
					   unsigned int *pxattr_id)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation getextattr_value_by_name
 */
/*
static fsal_status_t getextattr_value_by_name(struct fsal_obj_handle *obj_hdl,
					      const struct
					      req_op_context *opctx,
					      const char *xattr_name,
					      caddr_t buffer_addr,
					      size_t buffer_size,
					      size_t * p_output_size)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation getextattr_value_by_id
 */
/*
static fsal_status_t getextattr_value_by_id(struct fsal_obj_handle *obj_hdl,
					    const struct req_op_context *opctx,
					    unsigned int xattr_id,
					    caddr_t buffer_addr,
					    size_t buffer_size,
					    size_t *p_output_size)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation setextattr_value
 */
/*
static fsal_status_t setextattr_value(struct fsal_obj_handle *obj_hdl,
				      const struct req_op_context *opctx,
				      const char *xattr_name,
				      caddr_t buffer_addr,
				      size_t buffer_size,
				      int create)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation setextattr_value_by_id
 */
/*
static fsal_status_t setextattr_value_by_id(struct fsal_obj_handle *obj_hdl,
					    const struct req_op_context *opctx,
					    unsigned int xattr_id,
					    caddr_t buffer_addr,
					    size_t buffer_size)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation getextattr_attrs
 */
/*
static fsal_status_t getextattr_attrs(struct fsal_obj_handle *obj_hdl,
				      const struct req_op_context *opctx,
				      unsigned int xattr_id,
				      struct attrlist* p_attrs)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation remove_extattr_by_id
 */
/*
static fsal_status_t remove_extattr_by_id(struct fsal_obj_handle *obj_hdl,
					  const struct req_op_context *opctx,
					  unsigned int xattr_id)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation remove_extattr_by_name
 */
/*
static fsal_status_t remove_extattr_by_name(struct fsal_obj_handle *obj_hdl,
					    const struct req_op_context *opctx,
					    const char *xattr_name)
{
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
*/
/**
 * @brief Implements GLUSTER FSAL objectoperation lru_cleanup
 *
 * For now this function closed the fd if open as a part of the lru_cleanup.
 */

fsal_status_t lru_cleanup(struct fsal_obj_handle *obj_hdl,
			  lru_actions_t requests)
{
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	struct glusterfs_handle *objhandle =
	    container_of(obj_hdl, struct glusterfs_handle, handle);
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (objhandle->globalfd.glfd != NULL)
		status = file_close(obj_hdl);
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_lru_cleanup);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation handle_digest
 */

static fsal_status_t handle_digest(const struct fsal_obj_handle *obj_hdl,
				   fsal_digesttype_t output_type,
				   struct gsh_buffdesc *fh_desc)
{
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	size_t fh_size;
	struct glusterfs_handle *objhandle;
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	if (!fh_desc)
		return fsalstat(ERR_FSAL_FAULT, 0);

	objhandle = container_of(obj_hdl, struct glusterfs_handle, handle);

	switch (output_type) {
	case FSAL_DIGEST_NFSV3:
	case FSAL_DIGEST_NFSV4:
		fh_size = GLAPI_HANDLE_LENGTH;
		if (fh_desc->len < fh_size) {
			LogMajor(COMPONENT_FSAL,
				 "Space too small for handle.  need %zu, have %zu",
				 fh_size, fh_desc->len);
			status.major = ERR_FSAL_TOOSMALL;
			goto out;
		}
		memcpy(fh_desc->addr, objhandle->globjhdl, fh_size);
		break;
	default:
		status.major = ERR_FSAL_SERVERFAULT;
		goto out;
	}

	fh_desc->len = fh_size;
 out:
#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_handle_digest);
#endif
	return status;
}

/**
 * @brief Implements GLUSTER FSAL objectoperation handle_to_key
 */

static void handle_to_key(struct fsal_obj_handle *obj_hdl,
			  struct gsh_buffdesc *fh_desc)
{
	struct glusterfs_handle *objhandle;
#ifdef GLTIMING
	struct timespec s_time, e_time;

	now(&s_time);
#endif

	objhandle = container_of(obj_hdl, struct glusterfs_handle, handle);
	fh_desc->addr = objhandle->globjhdl;
	fh_desc->len = GLAPI_HANDLE_LENGTH;

#ifdef GLTIMING
	now(&e_time);
	latency_update(&s_time, &e_time, lat_handle_to_key);
#endif
}

/**
 * @brief Registers GLUSTER FSAL objectoperation vector
 */

void handle_ops_init(struct fsal_obj_ops *ops)
{
	ops->release = handle_release;
	ops->lookup = lookup;
	ops->create = create;
	ops->mkdir = makedir;
	ops->mknode = makenode;
	ops->readdir = read_dirents;
	ops->symlink = makesymlink;
	ops->readlink = readsymlink;
	ops->getattrs = getattrs;
	ops->setattrs = setattrs;
	ops->link = linkfile;
	ops->rename = renamefile;
	ops->unlink = file_unlink;
	ops->open = file_open;
	ops->status = file_status;
	ops->read = file_read;
	ops->write = file_write;
	ops->commit = commit;
	ops->lock_op = lock_op;
	ops->close = file_close;
	ops->lru_cleanup = lru_cleanup;
	ops->handle_digest = handle_digest;
	ops->handle_to_key = handle_to_key;

	/* fops with OpenTracking (multi-fd) enabled */
	ops->open2 = glusterfs_open2;
	ops->status2 = glusterfs_status2;
	ops->reopen2 = glusterfs_reopen2;
	ops->read2 = glusterfs_read2;
	ops->write2 = glusterfs_write2;
	ops->commit2 = glusterfs_commit2;
	ops->lock_op2 = glusterfs_lock_op2;
	ops->setattr2 = glusterfs_setattr2;
	ops->close2 = glusterfs_close2;


	/* pNFS related ops */
	handle_ops_pnfs(ops);
}
