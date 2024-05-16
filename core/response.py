import json


def ok(data=None):
    if data is None:
        response = {
            'errno': 0,
            'msg': '成功'
        }
        return response
    response = {
        'errno': 0,
        'data': data,
        'msg': '成功'
    }
    return response


def fail(errno, msg):
    response = {
        'errno': errno,
        'msg': msg
    }
    return response



