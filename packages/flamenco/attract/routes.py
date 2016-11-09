import functools
import logging

from flask import Blueprint, render_template
import flask_login

from pillar.web.utils import attach_project_pictures
import pillar.web.subquery
from pillar.web.system_util import pillar_api
import pillarsdk

from attract import current_attract
from attract.node_types.task import node_type_task
from attract.node_types.shot import node_type_shot

blueprint = Blueprint('attract', __name__)
log = logging.getLogger(__name__)


@blueprint.route('/')
def index():
    api = pillar_api()

    user = flask_login.current_user
    if user.is_authenticated:
        tasks = current_attract.task_manager.tasks_for_user(user.objectid)

    else:
        tasks = None

    # TODO: add projections.
    projects = current_attract.attract_projects()

    for project in projects['_items']:
        attach_project_pictures(project, api)

    projs_with_summaries = [
        (proj, current_attract.shot_manager.shot_status_summary(proj['_id']))
        for proj in projects['_items']
        ]

    # Fetch all activities for all Attract projects.
    id_to_proj = {p['_id']: p for p in projects['_items']}
    activities = pillarsdk.Activity.all({
        'where': {
            'project': {'$in': list(id_to_proj.keys())},
        },
        'sort': [('_created', -1)],
        'max_results': 20,
    }, api=api)

    # Fetch more info for each activity.
    for act in activities['_items']:
        act.actor_user = pillar.web.subquery.get_user_info(act.actor_user)
        act.project = id_to_proj[act.project]
        try:
            act.link = current_attract.link_for_activity(act)
        except ValueError:
            act.link = None

    return render_template('attract/index.html',
                           tasks=tasks,
                           projs_with_summaries=projs_with_summaries,
                           activities=activities)


def error_project_not_setup_for_attract():
    return render_template('attract/errors/project_not_setup.html')


def attract_project_view(extra_project_projections=None, extension_props=False):
    """Decorator, replaces the first parameter project_url with the actual project.

    Assumes the first parameter to the decorated function is 'project_url'. It then
    looks up that project, checks that it's set up for Attract, and passes it to the
    decorated function.

    If not set up for attract, uses error_project_not_setup_for_attract() to render
    the response.

    :param extra_project_projections: extra projections to use on top of the ones already
        used by this decorator.
    :type extra_project_projections: dict
    :param extension_props: whether extension properties should be included. Includes them
        in the projections, and verifies that they are there.
    :type extension_props: bool
    """

    from . import EXTENSION_NAME

    if callable(extra_project_projections):
        raise TypeError('Use with @attract_project_view() <-- note the parentheses')

    projections = {
        '_id': 1,
        'name': 1,
        'node_types': 1,
        # We don't need this here, but this way the wrapped function has access
        # to the orignal URL passed to it.
        'url': 1,
    }
    if extra_project_projections:
        projections.update(extra_project_projections)
    if extension_props:
        projections['extension_props.%s' % EXTENSION_NAME] = 1

    def decorator(wrapped):
        @functools.wraps(wrapped)
        def wrapper(project_url, *args, **kwargs):
            if isinstance(project_url, pillarsdk.Resource):
                # This is already a resource, so this call probably is from one
                # view to another. Assume the caller knows what he's doing and
                # just pass everything along.
                return wrapped(project_url, *args, **kwargs)

            api = pillar_api()

            project = pillarsdk.Project.find_by_url(
                project_url,
                {'projection': projections},
                api=api)

            is_attract = current_attract.is_attract_project(project,
                                                            test_extension_props=extension_props)
            if not is_attract:
                return error_project_not_setup_for_attract()

            if extension_props:
                pprops = project.extension_props.attract
                return wrapped(project, pprops, *args, **kwargs)
            return wrapped(project, *args, **kwargs)

        return wrapper

    return decorator


@blueprint.route('/<project_url>')
@attract_project_view(extension_props=True)
def project_index(project, attract_props):
    return render_template('attract/project.html',
                           project=project,
                           attract_props=attract_props)


@blueprint.route('/<project_url>/help')
@attract_project_view(extension_props=False)
def help(project):
    nt_task = project.get_node_type(node_type_task['name'])
    nt_shot = project.get_node_type(node_type_shot['name'])

    statuses = set(nt_task['dyn_schema']['status']['allowed'] +
                   nt_shot['dyn_schema']['status']['allowed'])

    return render_template('attract/help.html', statuses=statuses)
