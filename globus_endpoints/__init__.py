import base64
import datetime
import globus_sdk
import json
import posixpath
import requests
from girder import events, plugin
from girder.api import access, rest
from girder.constants import AccessType
from girder.exceptions import RestException
from girder.models.user import User
from girder_oauth.providers import Globus


def _globusFolder(id, name, path):
    info = json.dumps({
        'id': id,
        'path': path
    })
    return {
        '_id': 'globus:' + base64.b64encode(info),
        '_modelType': 'folder',
        '_accessLevel': AccessType.READ,
        'name': name,
        'globusFolder': True,
        'globusPath': path
    }


def _endpointFolder(ep):
    return _globusFolder(ep['id'], ep['display_name'], '~')  # TODO is this always relative to ~?


def _subFolder(info, f):
    return _globusFolder(info['id'], f['name'], posixpath.join(info['path'], f['name']))


def _item(info, f):
    path = posixpath.join(info['path'], f['name'])
    info = json.dumps({
        'id': info['id'],
        'path': path,
        'size': f['size'],
    })
    return {
        '_id': 'globus:' + base64.b64encode(info),
        '_modelType': 'item',
        '_accessLevel': AccessType.READ,
        'name': f['name'],
        'size': f['size'],
        'globusFile': True,
        'globusPath': path
    }


def _globusTc(user):
    authorizer = globus_sdk.AccessTokenAuthorizer(user['globusTransferToken'])
    return globus_sdk.TransferClient(authorizer=authorizer)


@access.public
@rest.boundHandler
def _globusChildFolders(self, event):
    params = event.info['params']
    user = self.getCurrentUser()

    if not user or 'parentId' not in params or 'globusTransferToken' not in user:
        return  # This is not a child listing request

    if params['parentId'].startswith('globus:'):
        # This folder represents a path inside a globus endpoint
        info = json.loads(base64.b64decode(params['parentId'][7:]))
        # TODO endpoint might be disabled
        itr = _globusTc(user).operation_ls(info['id'], path=info['path'])
        folders = [_subFolder(info, f) for f in itr if f['type'] == 'dir']
        event.preventDefault().addResponse(folders)
    elif user and str(user['_id']) == params['parentId'] and 'globusTransferToken' in user:
        # We're listing our own user folders, so we append globus endpoints
        tc = _globusTc(user)
        eps = [_endpointFolder(ep) for ep in tc.endpoint_search(filter_scope='shared-with-me')]  # TODO could also be 'my-endpoints'
        # TODO this overrides core behavior, we need to add the normal folders back in!
        event.preventDefault().addResponse(eps)


@access.public
@rest.boundHandler
def _globusChildItems(self, event):
    params = event.info['params']
    user = self.getCurrentUser()

    if not user or 'folderId' not in params or 'globusTransferToken' not in user:
        return

    if params['folderId'].startswith('globus:'):
        info = json.loads(base64.b64decode(params['folderId'][7:]))
        itr = _globusTc(user).operation_ls(info['id'], path=info['path'])
        folders = [_item(info, f) for f in itr if f['type'] == 'file']
        event.preventDefault().addResponse(folders)


@access.public
def _globusFolderInfo(event):
    id = event.info['id']
    if id.startswith('globus:'):
        info = json.loads(base64.b64decode(id[7:]))
        name = posixpath.basename(info['path'])
        event.preventDefault().addResponse(_globusFolder(info['id'], name, info['path']))


@access.public
@rest.boundHandler
def _globusItemInfo(self, event):
    id = event.info['id']
    if id.startswith('globus:'):
        user = self.getCurrentUser()
        info = json.loads(base64.b64decode(id[7:]))
        folderInfo = json.dumps({
            'id': info['id'],
            'path': posixpath.dirname(info['path']),
        })
        now = datetime.datetime.utcnow()
        event.preventDefault().addResponse({
            '_id': id,
            '_modelType': 'item',
            '_accessLevel': AccessType.READ,
            'name': posixpath.basename(info['path']),
            'size': info['size'],
            'creatorId': user['_id'],
            'baseParentId': user['_id'],
            'baseParentType': 'user',
            'created': now,
            'updated': now,
            'folderId': 'globus:' + base64.b64encode(folderInfo),
            'globusFile': True,
            'globusPath': info['path']
        })


@access.public
@rest.boundHandler
def _globusFileList(self, event):
    id = event.info['id']
    if id.startswith('globus:'):
        user = self.getCurrentUser()
        info = json.loads(base64.b64decode(id[7:]))
        now = datetime.datetime.utcnow()
        event.preventDefault().addResponse([{
            '_id': id,
            '_modelType': 'item',
            'name': posixpath.basename(info['path']),
            'size': info['size'],
            'creatorId': user['_id'],
            'exts': [],  # TODO?
            'created': now,
            'itemId': id,
            'globusFile': True,
            'globusPath': info['path']
        }])


@access.public
def _globusFolderDetails(event):
    if event.info['id'].startswith('globus:'):
        event.preventDefault().addResponse({
            'nFolders': None,
            'nItems': None
        })

@access.public
@rest.boundHandler
def _globusFileDownload(self, event):
    if not event.info['id'].startswith('globus:'):
        return

    info = json.loads(base64.b64decode(event.info['id'][7:]))
    path = info['path'][2:]  # TODO this strips the leading '~/', might need to be more robust
    r = requests.get(
        'https://%s.e.globus.org/%s' % (info['id'], path), stream=True, headers={
            'Authorization': 'Bearer ' + self.getCurrentUser()['globusDownloadToken']
        })
    try:
        r.raise_for_status()
    except requests.RequestException:
        raise RestException(
            'Invalid response from Globus HTTPS download service (%s).' % r.status_code, code=502)

    for h in {'Content-Length', 'Content-Type'}:
        if h in r.headers:
            rest.setResponseHeader(h, r.headers[h])

    disp = event.info['params'].get('contentDisposition', 'attachment')
    if disp == 'inline':
        rest.setResponseHeader('Content-Disposition', 'inline')
    else:
        basename = posixpath.basename(path)
        rest.setResponseHeader('Content-Disposition', 'attachment; filename="%s"' % basename)

    def stream():
        for chunk in r.iter_content(65536):
            yield chunk

    event.preventDefault().addResponse(stream)


def _saveGlobusToken(event):
    # When a user logs in with globus, we save their token
    if event.info['provider'] == Globus:
        transferToken = None
        downloadToken = None
        for other in event.info['token']['other_tokens']:
            if other.get('resource_server') == 'transfer.api.globus.org':
                transferToken = other['access_token']
            if other.get('resource_server') == 'petrel_https_server':  # TODO hardcoded
                downloadToken = other['access_token']

        User().update({
            '_id': event.info['user']['_id']
        }, {
            '$set': {
                'globusTransferToken': transferToken,
                'globusDownloadToken': downloadToken
            }
        }, multi=False)


class GirderPlugin(plugin.GirderPlugin):
    DISPLAY_NAME = 'Globus endpoints'

    def load(self, info):
        plugin.getPlugin('oauth').load(info)

        name = 'globus_endpoints'
        events.bind('rest.get.item.before', name, _globusChildItems)
        events.bind('rest.get.file/:id/download.before', name, _globusFileDownload)
        events.bind('rest.get.folder.before', name, _globusChildFolders)
        events.bind('rest.get.folder/:id.before', name, _globusFolderInfo)
        events.bind('rest.get.folder/:id/details.before', name, _globusFolderDetails)
        events.bind('rest.get.item/:id.before', name, _globusItemInfo)
        events.bind('rest.get.item/:id/download.before', name, _globusFileDownload)
        events.bind('rest.get.item/:id/files.before', name, _globusFileList)
        events.bind('oauth.auth_callback.after', name, _saveGlobusToken)
        # TODO folder rootpath
        # TODO item rootpath
        # TODO file GET
        # TODO file download
        # TODO item download

        Globus._AUTH_SCOPES += [
            'urn:globus:auth:scope:transfer.api.globus.org:all',
            'https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'  # petrel RS
        ]

        # TODO change access_type from 'online' to 'offline' to get a refresh token
