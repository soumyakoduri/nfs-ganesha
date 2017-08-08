/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright 2015-2017 Red Hat, Inc. and/or its affiliates.
 * Author: Daniel Gryniewicz <dang@redhat.com>
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
 * Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* file.c
 * File I/O methods for NULL module
 */

#include "config.h"

#include <assert.h>
#include "fsal.h"
#include "FSAL/access_check.h"
#include "fsal_convert.h"
#include <unistd.h>
#include <fcntl.h>
#include "FSAL/fsal_commonlib.h"
#include "mdcache_int.h"
#include "mdcache_lru.h"
#include "mdcache.h"

/**
 *
 * @brief Set a timestamp to the current time
 *
 * @param[out] time Pointer to time to be set
 *
 * @return true on success, false on failure
 *
 */
bool
mdc_set_time_current(struct timespec *time)
{
	struct timeval t;

	if (time == NULL)
		return false;

	if (gettimeofday(&t, NULL) != 0)
		return false;

	time->tv_sec = t.tv_sec;
	time->tv_nsec = 1000 * t.tv_usec;

	return true;
}

/**
 * @brief Open a file
 *
 * Delegate to sub-FSAL, subject to hard limits on the number of open FDs
 *
 * @param[in] obj_hdl	File to open
 * @param[in] openflags	Type of open to do
 * @return FSAL status
 */
fsal_status_t mdcache_open(struct fsal_obj_handle *obj_hdl,
			   fsal_openflags_t openflags)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	if (!mdcache_lru_fds_available()) {
		/* This seems the best idea, let the client try again later
		   after the reap. */
		return fsalstat(ERR_FSAL_DELAY, 0);
	}

	subcall(
		status = entry->sub_handle->obj_ops.open(
			entry->sub_handle, openflags)
	       );

	if (FSAL_IS_ERROR(status) && (status.major == ERR_FSAL_STALE))
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Re-open a file with different flags
 *
 * Delegate to sub-FSAL.  This should not be called unless the sub-FSAL supports
 * reopen.
 *
 * @param[in] obj_hdl	File to re-open
 * @param[in] openflags	New open flags
 * @return FSAL status
 */
fsal_status_t mdcache_reopen(struct fsal_obj_handle *obj_hdl,
			   fsal_openflags_t openflags)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.reopen(
			entry->sub_handle, openflags)
	       );

	if (FSAL_IS_ERROR(status) && (status.major == ERR_FSAL_STALE))
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Get the open status of a file
 *
 * Delegate to sub-FSAL, since this isn't cached metadata currently
 *
 * @param[in] obj_hdl	Object to check
 * @return Open flags indicating state
 */
fsal_openflags_t mdcache_status(struct fsal_obj_handle *obj_hdl)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_openflags_t status;

	subcall(
		status = entry->sub_handle->obj_ops.status(
			entry->sub_handle)
	       );

	return status;
}

/**
 * @brief Read from a file
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to read from
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of read buffer
 * @param[out] buffer	Buffer to read into
 * @param[out] read_amount	Amount read in bytes
 * @param[out] eof	true if End of File was hit
 * @return FSAL status
 */
fsal_status_t mdcache_read(struct fsal_obj_handle *obj_hdl, uint64_t offset,
			   size_t buf_size, void *buffer,
			   size_t *read_amount, bool *eof)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.read(
			entry->sub_handle, offset, buf_size, buffer,
			read_amount, eof)
	       );

	if (!FSAL_IS_ERROR(status))
		mdc_set_time_current(&entry->attrs.atime);
	else if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Read from a file w/ extra info
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to read from
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of read buffer
 * @param[out] buffer	Buffer to read into
 * @param[out] read_amount	Amount read in bytes
 * @param[out] eof	true if End of File was hit
 * @param[in,out] info	Extra info about read data
 * @return FSAL status
 */
fsal_status_t mdcache_read_plus(struct fsal_obj_handle *obj_hdl,
				uint64_t offset, size_t buf_size,
				void *buffer, size_t *read_amount,
				bool *eof, struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.read_plus(
			entry->sub_handle, offset, buf_size, buffer,
			read_amount, eof, info)
	       );

	if (!FSAL_IS_ERROR(status))
		mdc_set_time_current(&entry->attrs.atime);
	else if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Write to a file
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to write to
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of write buffer
 * @param[in] buffer	Buffer to write from
 * @param[out] write_amount	Amount written in bytes
 * @param[out] fsal_stable	true if write was to stable storage
 * @return FSAL status
 */
fsal_status_t mdcache_write(struct fsal_obj_handle *obj_hdl, uint64_t offset,
			    size_t buf_size, void *buffer,
			    size_t *write_amount, bool *fsal_stable)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.write(
			entry->sub_handle, offset, buf_size, buffer,
			write_amount, fsal_stable)
	       );

	if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);
	else if (!FSAL_IS_ERROR(status))
		atomic_clear_uint32_t_bits(&entry->mde_flags,
					MDCACHE_TRUST_ATTRS);

	return status;
}

/**
 * @brief Write to a file w/ extra info
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to read from
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of read buffer
 * @param[out] buffer	Buffer to read into
 * @param[out] read_amount	Amount read in bytes
 * @param[out] eof	true if End of File was hit
 * @param[in,out] info	Extra info about write data
 * @return FSAL status
 */
fsal_status_t mdcache_write_plus(struct fsal_obj_handle *obj_hdl,
				 uint64_t offset, size_t buf_size,
				 void *buffer, size_t *write_amount,
				 bool *fsal_stable, struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.write_plus(
			entry->sub_handle, offset, buf_size, buffer,
			write_amount, fsal_stable, info)
	       );

	if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Seek to data or hole
 *
 * Delegate to sub-FSAL
 *
 * @param[in]  obj_hdl      File to seek in
 * @param[in,out] info      Information about the data
 *
 * @return FSAL status.
 */
fsal_status_t mdcache_seek(struct fsal_obj_handle *obj_hdl,
			   struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.seek(entry->sub_handle,
							 info)
	       );

	return status;
}

/**
 * @brief IO Advise
 *
 * Delegate to sub-FSAL
 *
 * @param[in]  obj_hdl      File to be written
 * @param[in,out] info      Information about the data
 *
 * @return FSAL status.
 */
fsal_status_t mdcache_io_advise(struct fsal_obj_handle *obj_hdl,
				struct io_hints *hints)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.io_advise(
				entry->sub_handle, hints)
	       );

	return status;
}

/**
 * @brief Commit to a file
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to commit
 * @param[in] offset	Offset into file
 * @param[in] len	Length of commit
 * @return FSAL status
 */
fsal_status_t mdcache_commit(struct fsal_obj_handle *obj_hdl, off_t offset,
			     size_t len)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.commit(
			entry->sub_handle, offset, len)
	       );

	if (status.major == ERR_FSAL_STALE)
		mdcache_kill_entry(entry);
	else
		atomic_clear_uint32_t_bits(&entry->mde_flags,
					   MDCACHE_TRUST_ATTRS);

	return status;
}

/**
 * @brief Lock/unlock a range in a file
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	File to lock
 * @param[in] p_owner	Private data for lock
 * @param[in] lock_op	Lock operation
 * @param[in] req_lock	Parameters for requested lock
 * @param[in] conflicting_lock	Description of existing conflicting lock
 * @return FSAL status
 */
fsal_status_t mdcache_lock_op(struct fsal_obj_handle *obj_hdl,
			      void *p_owner, fsal_lock_op_t lock_op,
			      fsal_lock_param_t *req_lock,
			      fsal_lock_param_t *conflicting_lock)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.lock_op(
			entry->sub_handle, p_owner, lock_op, req_lock,
			conflicting_lock)
	       );

	return status;
}

/**
 * @brief Handle a share request
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	File to share
 * @param[in] p_owner	Private data for share
 * @param[in] param	Share request parameters
 * @return FSAL status
 */
fsal_status_t mdcache_share_op(struct fsal_obj_handle *obj_hdl, void *p_owner,
			       fsal_share_param_t param)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.share_op(
			entry->sub_handle, p_owner, param)
	       );

	return status;
}

/**
 * @brief Close a file
 *
 * @param[in] obj_hdl	File to close
 * @return FSAL status
 */
fsal_status_t mdcache_close(struct fsal_obj_handle *obj_hdl)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	/* XXX dang caching FDs?  How does it interact with multi-FD */
	subcall(
		status = entry->sub_handle->obj_ops.close(entry->sub_handle)
	       );

	return status;
}

static fsal_status_t mdc_open2_by_name(mdcache_entry_t *mdc_parent,
				       struct state_t *state,
				       fsal_openflags_t openflags,
				       enum fsal_create_mode createmode,
				       const char *name,
				       struct attrlist *attrib_set,
				       fsal_verifier_t verifier,
				       mdcache_entry_t **new_entry,
				       bool *caller_perm_check)
{
	fsal_status_t status;
	bool uncached = createmode >= FSAL_GUARDED;
	mdcache_entry_t *entry;
	struct fsal_obj_handle *sub_handle;

	*new_entry = NULL;

	if (!name)
		return fsalstat(ERR_FSAL_INVAL, 0);

	status = mdc_lookup(mdc_parent, name, uncached, &entry, NULL);

	if (FSAL_IS_ERROR(status)) {
		/* Does not exist, or other error, return to open2 to
		 * proceed if not found, otherwise to return the error.
		 */
		LogFullDebug(COMPONENT_CACHE_INODE,
			     "Lookup failed");
		return status;
	}

	/* Found to exist */
	if (createmode == FSAL_GUARDED) {
		mdcache_put(entry);
		return fsalstat(ERR_FSAL_EXIST, 0);
	} else if (createmode == FSAL_EXCLUSIVE) {
		/* Exclusive create with entry found, check verifier */
		if (!mdcache_check_verifier(&entry->obj_handle, verifier)) {
			/* Verifier check failed. */
			LogFullDebug(COMPONENT_CACHE_INODE,
				     "Verifier check failed.");
			mdcache_put(entry);
			return fsalstat(ERR_FSAL_EXIST, 0);
		}

		/* Verifier matches, go ahead and open the file. */

	} /* else UNGUARDED, go ahead and open the file. */

	subcall(
		status = entry->sub_handle->obj_ops.open2(
			entry->sub_handle, state, openflags, createmode,
			NULL, attrib_set, verifier, &sub_handle,
			NULL, caller_perm_check)
	       );

	if (FSAL_IS_ERROR(status)) {
		/* Open failed. */
		LogFullDebug(COMPONENT_CACHE_INODE,
			     "Open failed %s",
			     msg_fsal_err(status.major));
		mdcache_put(entry);
	} else {
		LogFullDebug(COMPONENT_CACHE_INODE,
			     "Opened entry %p, sub_handle %p",
			     entry, entry->sub_handle);
		if (openflags & FSAL_O_TRUNC) {
			/* Invalidate the attributes since we just truncated. */
			atomic_clear_uint32_t_bits(&entry->mde_flags,
						   MDCACHE_TRUST_ATTRS);
		}
		*new_entry = entry;
	}

	return status;
}

/**
 * @brief Open a file descriptor for read or write and possibly create
 *
 * This function opens a file for read or write, possibly creating it.
 * If the caller is passing a state, it must hold the state_lock
 * exclusive.
 *
 * state can be NULL which indicates a stateless open (such as via the
 * NFS v3 CREATE operation), in which case the FSAL must assure protection
 * of any resources. If the file is being created, such protection is
 * simple since no one else will have access to the object yet, however,
 * in the case of an exclusive create, the common resources may still need
 * protection.
 *
 * If Name is NULL, obj_hdl is the file itself, otherwise obj_hdl is the
 * parent directory.
 *
 * On an exclusive create, the upper layer may know the object handle
 * already, so it MAY call with name == NULL. In this case, the caller
 * expects just to check the verifier.
 *
 * On a call with an existing object handle for an UNCHECKED create,
 * we can set the size to 0.
 *
 * At least the mode attribute must be set if createmode is FSAL_UNCHECKED,
 * FSAL_GUARDED, FSAL_EXCLUSIVE_41, or FSAL_EXCLUSIVE_9P.
 *
 * If an open by name succeeds and did not result in Ganesha creating a file,
 * the caller will need to do a subsequent permission check to confirm the
 * open. This is because the permission attributes were not available
 * beforehand.
 *
 * The caller is expected to invoke fsal_release_attrs to release any
 * resources held by the set attributes. The FSAL layer MAY have added an
 * inherited ACL.
 *
 * The caller will set the request_mask in attrs_out to indicate the attributes
 * of interest. ATTR_ACL SHOULD NOT be requested and need not be provided. If
 * not all the requested attributes can be provided, this method MUST return
 * an error unless the ATTR_RDATTR_ERR bit was set in the request_mask.
 *
 * Since this method may instantiate a new fsal_obj_handle, it will be forced
 * to fetch at least some attributes in order to even know what the object
 * type is (as well as it's fileid and fsid). For this reason, the operation
 * as a whole can be expected to fail if the attributes were not able to be
 * fetched.
 *
 * The attributes will not be returned if this is an open by object as
 * opposed to an open by name.
 *
 * @note If the file was created, @a new_obj has been ref'd
 *
 * @param[in] obj_hdl               File to open or parent directory
 * @param[in,out] state             state_t to use for this operation
 * @param[in] openflags             Mode for open
 * @param[in] createmode            Mode for create
 * @param[in] name                  Name for file if being created or opened
 * @param[in] attrs_in              Attributes to set on created file
 * @param[in] verifier              Verifier to use for exclusive create
 * @param[in,out] new_obj           Newly created object
 * @param[in,out] attrs_out         Optional attributes for newly created object
 * @param[in,out] caller_perm_check The caller must do a permission check
 *
 * @return FSAL status.
 */

fsal_status_t mdcache_open2(struct fsal_obj_handle *obj_hdl,
			   struct state_t *state,
			   fsal_openflags_t openflags,
			   enum fsal_create_mode createmode,
			   const char *name,
			   struct attrlist *attrs_in,
			   fsal_verifier_t verifier,
			   struct fsal_obj_handle **new_obj,
			   struct attrlist *attrs_out,
			   bool *caller_perm_check)
{
	mdcache_entry_t *mdc_parent =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	mdcache_entry_t *new_entry = NULL;
	struct fsal_obj_handle *sub_handle = NULL;
	fsal_status_t status;
	struct attrlist attrs;
	const char *dispname = name != NULL ? name : "<by-handle>";
	struct mdcache_fsal_export *export = mdc_cur_export();
	bool invalidate;

	LogAttrlist(COMPONENT_CACHE_INODE, NIV_FULL_DEBUG,
		    "attrs_in ", attrs_in, false);

	if (name) {
		if (!state && !mdcache_lru_fds_available()) {
			/* This seems the best idea, let the
			 * client try again later after the reap.
			 */
			return fsalstat(ERR_FSAL_DELAY, 0);
		}

		/* Check if we have the file already cached, in which case
		 * we can open by object instead of by name.
		 */
		status = mdc_open2_by_name(mdc_parent, state, openflags,
					   createmode, name, attrs_in,
					   verifier, &new_entry,
					   caller_perm_check);

		if (status.major == ERR_FSAL_NO_ERROR) {
			/* Return the newly opened file. */
			*new_obj = &new_entry->obj_handle;

			if (openflags & FSAL_O_TRUNC) {
				/* Mark the attributes as not-trusted, so we
				 * will refresh the attributes on the next
				 * getattrs.
				 */
				atomic_clear_uint32_t_bits(
				    &new_entry->mde_flags, MDCACHE_TRUST_ATTRS);
			}

			return status;
		}

		if (status.major != ERR_FSAL_NOENT) {
			/* Return the error */
			*new_obj = NULL;
			return status;
		}
	}

	/* Ask for all supported attributes except ACL (we defer fetching ACL
	 * until asked for it (including a permission check).
	 *
	 * We can survive if we don't actually succeed in fetching the
	 * attributes.
	 */
	fsal_prepare_attrs(&attrs,
			   (op_ctx->fsal_export->exp_ops.
				   fs_supported_attrs(op_ctx->fsal_export)
				& ~ATTR_ACL) | ATTR_RDATTR_ERR);

	subcall(
		status = mdc_parent->sub_handle->obj_ops.open2(
			mdc_parent->sub_handle, state, openflags, createmode,
			name, attrs_in, verifier, &sub_handle, &attrs,
			caller_perm_check)
	       );

	if (unlikely(FSAL_IS_ERROR(status))) {
		LogDebug(COMPONENT_CACHE_INODE,
			 "open2 %s failed with %s",
			 dispname, fsal_err_txt(status));
		if (status.major == ERR_FSAL_STALE) {
			/* If we got ERR_FSAL_STALE, the previous FSAL call
			 * must have failed with a bad parent.
			 */
			mdcache_kill_entry(mdc_parent);
		}
		fsal_release_attrs(&attrs);
		*new_obj = NULL;
		return status;
	}

	if (!name) {
		/* Wasn't a create and/or entry already found */
		if (openflags & FSAL_O_TRUNC) {
			/* Mark the attributes as not-trusted, so we will
			 * refresh the attributes.
			 */
			atomic_clear_uint32_t_bits(&mdc_parent->mde_flags,
						   MDCACHE_TRUST_ATTRS);
		}

		LogFullDebug(COMPONENT_CACHE_INODE,
			     "Open2 of object succeeded.");
		*new_obj = obj_hdl;
		/* We didn't actually get any attributes, but release anyway
		 * for code consistency.
		 */
		fsal_release_attrs(&attrs);
		return status;
	}

	invalidate = createmode != FSAL_NO_CREATE;

	PTHREAD_RWLOCK_wrlock(&mdc_parent->content_lock);

	/* We will invalidate parent attrs if we did any form of create. */
	status = mdcache_alloc_and_check_handle(export, sub_handle,
						new_obj, false,
						&attrs, attrs_out,
						"open2 ", mdc_parent, name,
						&invalidate,
						state);

	PTHREAD_RWLOCK_unlock(&mdc_parent->content_lock);

	fsal_release_attrs(&attrs);

	if (createmode != FSAL_NO_CREATE && !invalidate) {
		/* Refresh destination directory attributes without
		 * invalidating dirents.
		 */
		mdcache_refresh_attrs_no_invalidate(mdc_parent);
	}

	return status;
}

/**
 * @brief Check the verifier
 *
 * @param[in] obj_hdl	File to check
 * @param[in] verifier	Verifier to check
 * @return FSAL status
 */
bool mdcache_check_verifier(struct fsal_obj_handle *obj_hdl,
				     fsal_verifier_t verifier)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	bool result;

	/* XXX dang caching FDs?  How does it interact with multi-FD */
	subcall(
		result = entry->sub_handle->obj_ops.check_verifier(
				entry->sub_handle, verifier)
	       );

	return result;
}

/**
 * @brief Get the open status of a file (new style)
 *
 * Delegate to sub-FSAL, since this isn't cached metadata currently
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to check
 * @return Open flags indicating state
 */
fsal_openflags_t mdcache_status2(struct fsal_obj_handle *obj_hdl,
				 struct state_t *state)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_openflags_t status;

	subcall(
		status = entry->sub_handle->obj_ops.status2(
			entry->sub_handle, state)
	       );

	return status;
}

/**
 * @brief Re-open a file with different flags (new style)
 *
 * Delegate to sub-FSAL.  This should not be called unless the sub-FSAL supports
 * reopen2.
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to re-open
 * @param[in] openflags	New open flags
 * @return FSAL status
 */
fsal_status_t mdcache_reopen2(struct fsal_obj_handle *obj_hdl,
			      struct state_t *state,
			      fsal_openflags_t openflags)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;
	bool truncated = openflags & FSAL_O_TRUNC;

	subcall(
		status = entry->sub_handle->obj_ops.reopen2(
			entry->sub_handle, state, openflags)
	       );

	if (FSAL_IS_ERROR(status) && (status.major == ERR_FSAL_STALE))
		mdcache_kill_entry(entry);

	if (truncated && !FSAL_IS_ERROR(status)) {
		atomic_clear_uint32_t_bits(&entry->mde_flags,
					   MDCACHE_TRUST_ATTRS);
	}

	return status;
}

/**
 * @brief Read from a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] bypass	Bypass deny read
 * @param[in] state	Open file state to read
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of read buffer
 * @param[in,out] buffer	Buffer to read into
 * @param[out] read_amount	Amount read in bytes
 * @param[out] eof	true if End of File was hit
 * @param[in] info	io_info for READ_PLUS
 * @return FSAL status
 */
fsal_status_t mdcache_read2(struct fsal_obj_handle *obj_hdl,
			   bool bypass,
			   struct state_t *state,
			   uint64_t offset,
			   size_t buf_size,
			   void *buffer,
			   size_t *read_amount,
			   bool *eof,
			   struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.read2(
			entry->sub_handle, bypass, state, offset, buf_size,
			buffer, read_amount, eof, info)
	       );

	if (!FSAL_IS_ERROR(status))
		mdc_set_time_current(&entry->attrs.atime);
	else if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Write to a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] bypass	Bypass any non-mandatory deny write
 * @param[in] state	Open file state to write
 * @param[in] offset	Offset into file
 * @param[in] buf_size	Size of write buffer
 * @param[in] buffer	Buffer to write from
 * @param[out] write_amount	Amount written in bytes
 * @param[out] fsal_stable	true if write was to stable storage
 * @param[in] info	io_info for WRITE_PLUS
 * @return FSAL status
 */
fsal_status_t mdcache_write2(struct fsal_obj_handle *obj_hdl,
			     bool bypass,
			     struct state_t *state,
			     uint64_t offset,
			     size_t buf_size,
			     void *buffer,
			     size_t *write_amount,
			     bool *fsal_stable,
			     struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.write2(
			entry->sub_handle, bypass, state, offset, buf_size,
			buffer, write_amount, fsal_stable, info)
	       );

	if (status.major == ERR_FSAL_STALE)
		mdcache_kill_entry(entry);
	else
		atomic_clear_uint32_t_bits(&entry->mde_flags,
					   MDCACHE_TRUST_ATTRS);

	return status;
}

/**
 * @brief Seek within a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to seek within
 * @param[in] info	io_info for seek
 * @return FSAL status
 */
fsal_status_t mdcache_seek2(struct fsal_obj_handle *obj_hdl,
			    struct state_t *state,
			    struct io_info *info)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.seek2(
			entry->sub_handle, state, info)
	       );

	if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Advise access pattern for a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to advise on
 * @param[in] hints	io_hints containing advice
 * @return FSAL status
 */
fsal_status_t mdcache_io_advise2(struct fsal_obj_handle *obj_hdl,
				 struct state_t *state,
				 struct io_hints *hints)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.io_advise2(
			entry->sub_handle, state, hints)
	       );

	if (status.major == ERR_FSAL_DELAY)
		mdcache_kill_entry(entry);

	return status;
}

/**
 * @brief Commit to a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object to commit
 * @param[in] offset	Offset into file
 * @param[in] len	Length of commit
 * @return FSAL status
 */
fsal_status_t mdcache_commit2(struct fsal_obj_handle *obj_hdl, off_t offset,
			      size_t len)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.commit2(
			entry->sub_handle, offset, len)
	       );

	if (status.major == ERR_FSAL_STALE)
		mdcache_kill_entry(entry);
	else
		atomic_clear_uint32_t_bits(&entry->mde_flags,
					   MDCACHE_TRUST_ATTRS);

	return status;
}

/**
 * @brief Lock/unlock a range in a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to lock/unlock
 * @param[in] p_owner	Private data for lock
 * @param[in] lock_op	Lock operation
 * @param[in] req_lock	Parameters for requested lock
 * @param[in] conflicting_lock	Description of existing conflicting lock
 * @return FSAL status
 */
fsal_status_t mdcache_lock_op2(struct fsal_obj_handle *obj_hdl,
			      struct state_t *state,
			      void *p_owner,
			      fsal_lock_op_t lock_op,
			      fsal_lock_param_t *req_lock,
			      fsal_lock_param_t *conflicting_lock)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.lock_op2(
			entry->sub_handle, state, p_owner, lock_op, req_lock,
			conflicting_lock)
	       );

	return status;
}

/**
 * @brief Get/Release delegation for a file (new style)
 *
 * Delegate to sub-FSAL
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to get/release
 * @param[in] p_owner	Private owner
 * @param[in] deleg_op	Delegation operation
 * @param[in] req_params  Parameters for requested delegation
 * @return FSAL status
 */
fsal_status_t mdcache_lease_op2(struct fsal_obj_handle *obj_hdl,
				struct state_t *state,
				void *p_owner,
				fsal_deleg_op_t deleg_op,
				fsal_deleg_param_t *req_params)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.lease_op2(
			entry->sub_handle, state, p_owner, deleg_op,
			req_params);
	       );

	return status;
}

/**
 * @brief Close a file (new style)
 *
 * @param[in] obj_hdl	Object owning state
 * @param[in] state	Open file state to close
 * @return FSAL status
 */
fsal_status_t mdcache_close2(struct fsal_obj_handle *obj_hdl,
			     struct state_t *state)
{
	mdcache_entry_t *entry =
		container_of(obj_hdl, mdcache_entry_t, obj_handle);
	fsal_status_t status;

	subcall(
		status = entry->sub_handle->obj_ops.close2(
			  entry->sub_handle, state)
	       );

	if (test_mde_flags(entry, MDCACHE_UNREACHABLE) &&
	    !mdc_has_state(entry)) {
		/* Entry was marked unreachable, and last state is gone */
		(void)mdcache_kill_entry(entry);
	}

	return status;
}
