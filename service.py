import os
import time
from datetime import datetime, timezone

from waitress import serve
from flask_cors import CORS
from flask import Flask, send_file

from werkzeug.utils import secure_filename
from flask_restplus import Resource, Api, reqparse
from werkzeug.datastructures import FileStorage

from db_driver import insert_into_userdata, login, upload_file, get_data


def current_milli_time():
    return round(time.time() * 1000)


def store_and_verify_file(file_from_request, work_dir):
    if not file_from_request.filename:
        return -1, 'Empty file part provided!'
    try:
        file_path = os.path.join(work_dir, secure_filename(file_from_request.filename))
        if os.path.exists(file_path) is False:
            file_from_request.save(file_path)
        return 0, file_path
    except Exception as ex:
        return -1, str(ex)


def upload_and_verify_file(file_from_request, work_dir):
    if not file_from_request.filename:
        return -1, 'Empty file part provided!', None
    try:
        fn = str(current_milli_time()) + '_' + secure_filename(file_from_request.filename)
        file_path = os.path.join(work_dir, fn)
        if os.path.exists(file_path) is False:
            file_from_request.save(file_path)
        return 0, file_path, fn
    except Exception as ex:
        return -1, str(ex), None


def create_app():
    app = Flask("foo", instance_relative_config=True)

    api = Api(
        app,
        version='1.0.0',
        title='Hops Backend App',
        description='Hops Backend App',
        default='Hops Backend App',
        default_label=''
    )

    CORS(app)

    get_parser = reqparse.RequestParser()
    get_parser.add_argument('offset',
                            type=int,
                            help='Offset from which records needs to be read',
                            required=True)
    get_parser.add_argument('count',
                            type=int,
                            help='Number of records to be read from offset',
                            required=True)

    @api.route('/list')
    @api.expect(get_parser)
    class ListService(Resource):
        @api.expect(get_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = get_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            try:
                offset = args['offset']
                count = args['count']
                ret, ret_str = get_data(offset, count)
                if ret_str != "Success":
                    rv = dict()
                    rv['status'] = ret_str
                    return rv, 404
                else:
                    return ret, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    upload_parser = reqparse.RequestParser()
    upload_parser.add_argument('userid',
                               type=str,
                               help='The User ID',
                               required=True)
    upload_parser.add_argument('filetype',
                               type=str,
                               help='The type of file uploaded - CTScan, X-Ray, MRI etc.',
                               required=True)
    upload_parser.add_argument('file',
                               location='files',
                               type=FileStorage,
                               help='The file to upload',
                               required=True)

    @api.route('/upload_file')
    @api.expect(upload_parser)
    class UploadService(Resource):
        @api.expect(upload_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = upload_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _userid = args['userid']
                _filetype = args['filetype']
                _file = args['file']
                ret, status_or_filepath, none_or_fileid = upload_and_verify_file(_file, work_dir='uploads')
                if ret == 0:
                    _filepath = status_or_filepath
                    _fileid = upload_file(_userid, _filetype, _filepath)
                    _datetime = datetime.now(timezone.utc)
                    rv = dict()
                    rv['fileid'] = none_or_fileid
                    rv['datetime'] = str(_datetime)
                    return rv, 200
                else:
                    print(status_or_filepath)
                    rv = dict()
                    rv['status'] = str(status_or_filepath)
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    login_user_parser = reqparse.RequestParser()
    login_user_parser.add_argument('userid',
                                   type=str,
                                   help='The User ID',
                                   required=True)
    login_user_parser.add_argument('password',
                                   type=str,
                                   help='The User password',
                                   required=True)

    @api.route('/login')
    @api.expect(login_user_parser)
    class LoginService(Resource):
        @api.expect(login_user_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = login_user_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _userid = args['userid']
                _password = args['password']
                rec = login(_userid, _password)
                return rec, 200
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    register_user_parser = reqparse.RequestParser()
    register_user_parser.add_argument('userid',
                                      type=str,
                                      help='The User ID',
                                      required=True)
    register_user_parser.add_argument('password',
                                      type=str,
                                      help='The User password',
                                      required=True)
    register_user_parser.add_argument('fullname',
                                      type=str,
                                      help='The User Full Name',
                                      required=True)
    register_user_parser.add_argument('typeofuser',
                                      type=str,
                                      help='The Type of User - Personal / Hospital',
                                      required=True)
    register_user_parser.add_argument('username',
                                      type=str,
                                      help='The User Name',
                                      required=True)
    register_user_parser.add_argument('purpose',
                                      type=str,
                                      help='The purpose',
                                      required=True)
    register_user_parser.add_argument('age',
                                      type=str,
                                      help='The User age',
                                      required=True)
    register_user_parser.add_argument('relationship',
                                      type=str,
                                      help='The relationship with the patient',
                                      required=False)
    register_user_parser.add_argument('mobilenumber',
                                      type=str,
                                      help='The User Mobile Number',
                                      required=False)
    register_user_parser.add_argument('gender',
                                      type=str,
                                      help='Gender',
                                      required=False)
    register_user_parser.add_argument('birthdate',
                                      type=str,
                                      help='The birth date',
                                      required=False)
    register_user_parser.add_argument('emailaddress',
                                      type=str,
                                      help='The User email address',
                                      required=False)
    register_user_parser.add_argument('photo',
                                      location='files',
                                      type=FileStorage,
                                      help='The user photo file',
                                      required=False)
    register_user_parser.add_argument('patientid',
                                      type=str,
                                      help='If typeofuser is Personal patientid is required, '
                                           'If typeofuser is Hospital patientid optional',
                                      required=False)

    @api.route('/save_patient_details')
    @api.expect(register_user_parser)
    class RegisterService(Resource):
        @api.expect(register_user_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = register_user_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404
            try:
                _userid = args['userid']
                _password = args['password']
                _fullname = args['fullname']
                _typeofuser = args['typeofuser']
                _username = args['username']
                _purpose = args['purpose']
                _age = args['age']

                try:
                    _relationship = args['relationship']
                    if _relationship is None:
                        _relationship = ''
                except Exception as e:
                    _relationship = ''

                try:
                    _mobilenumber = args['mobilenumber']
                    if _mobilenumber is None:
                        _mobilenumber = ''
                except Exception as e:
                    _mobilenumber = ''

                try:
                    _gender = args['gender']
                    if _gender is None:
                        _gender = ''
                except Exception as e:
                    _gender = ''

                try:
                    _birthdate = args['birthdate']
                    if _birthdate is None:
                        _birthdate = ''
                except Exception as e:
                    _birthdate = ''

                try:
                    _emailaddress = args['emailaddress']
                    if _emailaddress is None:
                        _emailaddress = ''
                except Exception as e:
                    _emailaddress = ''

                try:
                    _photo = args['photo']
                    ret, status_or_filepath = store_and_verify_file(_photo, work_dir='photo')
                    if ret == 0:
                        _photo = status_or_filepath
                    else:
                        print(status_or_filepath)
                        _photo = ''
                    if _photo is None:
                        _photo = ''
                except Exception as e:
                    _photo = ''

                if _typeofuser == 'Personal':
                    _patientid = args['patientid']
                else:
                    try:
                        _patientid = args['patientid']
                        if _patientid is None:
                            _patientid = ''
                    except Exception as e:
                        _patientid = ''

                ret, status = insert_into_userdata(_userid,
                                                   _password,
                                                   _fullname,
                                                   _typeofuser,
                                                   _username,
                                                   _purpose,
                                                   _age,
                                                   _relationship,
                                                   _mobilenumber,
                                                   _gender,
                                                   _birthdate,
                                                   _emailaddress,
                                                   _photo,
                                                   _patientid)
                rv = dict()
                rv['status'] = status
                if ret == 0:
                    return rv, 200
                else:
                    return rv, 404
            except Exception as e:
                rv = dict()
                rv['status'] = str(e)
                return rv, 404

    health_check_parser = reqparse.RequestParser()
    health_check_parser.add_argument('var',
                                     type=int,
                                     help='dummy variable',
                                     required=True)

    @api.route('/health_check')
    @api.expect(health_check_parser)
    class HealthCheckService(Resource):
        @api.expect(health_check_parser)
        @api.doc(responses={"response": 'json'})
        def post(self):
            try:
                args = health_check_parser.parse_args()
            except Exception as e:
                rv = dict()
                rv['health'] = str(e)
                return rv, 404
            rv = dict()
            rv['health'] = "good"
            return rv, 200

    return app


if __name__ == "__main__":
    serve(create_app(), host='0.0.0.0', port=8000, threads=20)
