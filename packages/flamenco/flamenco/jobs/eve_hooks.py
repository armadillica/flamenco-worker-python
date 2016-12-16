# -*- encoding: utf-8 -*-

import logging

import werkzeug.exceptions as wz_exceptions

from pillar.api.utils.authorization import check_permissions, user_has_role

log = logging.getLogger(__name__)


def after_inserting_jobs(jobs):
    from flamenco import job_compilers

    for job in jobs:
        # Prepare storage dir for the job files?
        # Generate tasks
        log.info('Generating tasks for job {}'.format(job['_id']))
        job_compilers.compile_job(job)


def check_job_permission_fetch(job_doc):
    from flamenco import current_flamenco

    if user_has_role(u'admin'):
        return

    if not current_flamenco.manager_manager.user_is_manager():
        # FIXME: Regular user, undefined behaviour as of yet.
        # # Run validation process, since GET on nodes entry point is public
        # check_permissions('flamenco_jobs', job_doc, 'GET',
        #                   append_allowed_methods=True)
        raise wz_exceptions.Forbidden()

    mngr_doc_id = job_doc.get('manager')
    if not current_flamenco.manager_manager.user_manages(mngr_doc_id=mngr_doc_id):
        raise wz_exceptions.Forbidden()


def check_job_permission_fetch_resource(response):
    from flamenco import current_flamenco
    from pylru import lrudecorator

    if user_has_role(u'admin'):
        return

    if not current_flamenco.manager_manager.user_is_manager():
        # FIXME: Regular user, undefined behaviour as of yet.
        # # Run validation process, since GET on nodes entry point is public
        # check_permissions('flamenco_jobs', job_doc, 'GET',
        #                   append_allowed_methods=True)
        raise wz_exceptions.Forbidden()

    @lrudecorator(32)
    def user_managers(mngr_doc_id):
        return current_flamenco.manager_manager.user_manages(mngr_doc_id=mngr_doc_id)

    items = response['_items']
    to_remove = []
    for idx, job_doc in enumerate(items):
        if not user_managers(job_doc.get('manager')):
            to_remove.append(idx)

    for idx in reversed(to_remove):
        del items[idx]

    response['_meta']['total'] -= len(items)


def check_job_permissions_modify(job_doc, original_doc=None):
    """For now, only admins are allowed to create, edit, and delete jobs."""

    if not user_has_role(u'admin'):
        raise wz_exceptions.Forbidden()

    # FIXME: check user access to the project.


def setup_app(app):
    app.on_inserted_flamenco_jobs = after_inserting_jobs
    app.on_fetched_item_flamenco_jobs += check_job_permission_fetch
    app.on_fetched_resource_flamenco_jobs += check_job_permission_fetch_resource

    app.on_insert_flamenco_jobs += check_job_permissions_modify
    app.on_update_flamenco_jobs += check_job_permissions_modify
    app.on_replace_flamenco_jobs += check_job_permissions_modify
    app.on_delete_flamenco_jobs += check_job_permissions_modify
