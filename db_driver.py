import os.path
import psycopg2
from datetime import datetime, timezone


def get_parameter(userid, parameter_name):
    ret = 0
    ret_str = ""
    conn = None
    sql = "SELECT " + parameter_name + " FROM user_data WHERE userid = %s;"
    try:
        conn = psycopg2.connect(host="localhost",
                                database="hops-ai",
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql, (userid,))
        records = cur.fetchall()
        cur.close()
        if len(records) == 1:
            ret_str = str(records[0][0])
        else:
            ret = -1
            ret_str = 'Not Found'
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        ret_str = str(error)
        ret = -1
    finally:
        if conn is not None:
            conn.close()
        return ret_str, ret


def update_parameter(userid, var_name, var_value):
    ret = 0
    ret_str = "Success"
    conn = None
    sql = "UPDATE user_data SET " + var_name + " = %s WHERE userid = %s;"

    try:
        conn = psycopg2.connect(host="localhost",
                                database="hops-ai",
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql, (var_value, userid,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        ret_str = str(error)
        ret = -1
    finally:
        if conn is not None:
            conn.close()
        return ret_str, ret


def get_data(offset, count):
    ret = dict()
    ret_str = "Success"
    conn = None
    sql = """SELECT * FROM user_data OFFSET %s ROWS FETCH FIRST %s ROW ONLY;"""

    try:
        conn = psycopg2.connect(host="localhost",
                                database="hops-ai",
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql, (offset, count,))
        records = cur.fetchall()
        idx = 0
        for record in records:
            one_rec = dict()
            files = str(record[16]).split(',')
            files_id = ''
            files_uploaded_status = ''
            for file in files:
                if os.path.exists(file):
                    files_uploaded_status = files_uploaded_status + ',Uploaded'
                else:
                    if len(file) > 0:
                        files_uploaded_status = files_uploaded_status + ',Missing'
                files_id = files_id + ',' + os.path.basename(file)
            files_id = files_id[1:]
            files_uploaded_status = files_uploaded_status[1:]
            update_parameter(str(record[0]), var_name='filesuploadedstatus', var_value=files_uploaded_status)
            one_rec['userid'] = str(record[0])
            one_rec['fullname'] = str(record[2])
            one_rec['filesid'] = files_id
            one_rec['relationship'] = str(record[7])
            one_rec['patientid'] = str(record[14])
            one_rec['emailaddress'] = str(record[12])
            one_rec['mobilenumber'] = str(record[8])
            one_rec['age'] = str(record[6])
            one_rec['gender'] = str(record[9])
            one_rec['filetypes'] = str(record[15])
            one_rec['filesuploadstatus'] = files_uploaded_status
            one_rec['purpose'] = str(record[5])
            ret[str(idx)] = one_rec
            idx += 1
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        ret_str = str(error)
        ret = dict()
    finally:
        if conn is not None:
            conn.close()
        return ret, ret_str


def upload_file(userid, filetype, filepath):
    ret_str, ret = get_parameter(userid, parameter_name='filetypes')
    if ret == 0:
        param_val_filetypes = ret_str
        param_val_filetypes = param_val_filetypes + ',' + filetype
        if param_val_filetypes[0] == ',':
            param_val_filetypes = param_val_filetypes[1:]
        if param_val_filetypes[-1] == ',':
            param_val_filetypes = param_val_filetypes[:-1]
        update_parameter(userid, var_name='filetypes', var_value=param_val_filetypes)

    ret_str, ret = get_parameter(userid, parameter_name='filesuploaded')
    if ret == 0:
        param_val_filesuploaded = ret_str
        param_val_filesuploaded = param_val_filesuploaded + ',' + filepath
        if param_val_filesuploaded[0] == ',':
            param_val_filesuploaded = param_val_filesuploaded[1:]
        if param_val_filesuploaded[-1] == ',':
            param_val_filesuploaded = param_val_filesuploaded[:-1]
        update_parameter(userid, var_name='filesuploaded', var_value=param_val_filesuploaded)

    ret_str, ret = get_parameter(userid, parameter_name='filesuploadedstatus')
    if ret == 0:
        param_val_filesuploadedstatus = ret_str
        param_val_filesuploadedstatus = param_val_filesuploadedstatus + ',Uploaded'
        if param_val_filesuploadedstatus[0] == ',':
            param_val_filesuploadedstatus = param_val_filesuploadedstatus[1:]
        if param_val_filesuploadedstatus[-1] == ',':
            param_val_filesuploadedstatus = param_val_filesuploadedstatus[:-1]
        update_parameter(userid, var_name='filesuploadedstatus', var_value=param_val_filesuploadedstatus)

    return filepath


def login(userid, password):
    rec = dict()
    conn = None
    sql = """select * from user_data where userid = %s"""
    try:
        conn = psycopg2.connect(host="localhost",
                                database="hops-ai",
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        cur.execute(sql, (userid,))
        record = cur.fetchall()
        if len(record) != 1:
            rec = dict()
        else:
            record = record[0]
            if str(record[1]) != password:
                rec = dict()
            else:
                rec['userid'] = str(record[0])
                rec['fullname'] = str(record[2])
                rec['typeofuser'] = str(record[3])
                rec['username'] = str(record[4])
                rec['mobilenumber'] = str(record[8])
                rec['gender'] = str(record[9])
                rec['birthdate'] = str(record[10])
                rec['emailaddress'] = str(record[12])
                rec['patientId'] = str(record[14])
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        rec = dict()
    finally:
        if conn is not None:
            conn.close()
        return rec


def insert_into_userdata(userid,
                         password,
                         fullname,
                         typeofuser,
                         username,
                         purpose,
                         age,
                         relationship,
                         mobilenumber,
                         gender,
                         birthdate,
                         emailaddress,
                         photo,
                         patientid):
    ret = 0
    status = "Success"
    conn = None
    sql = """INSERT INTO user_data(userid, password, fullname, typeofuser, username, purpose, age, relationship, 
             mobilenumber, gender, birthdate, creationdate, emailaddress, photo, patientId, filetypes, 
             filesuploaded, filesuploadedstatus) 
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING userid;"""
    try:
        conn = psycopg2.connect(host="localhost",
                                database="hops-ai",
                                user="postgres",
                                password="admin")
        cur = conn.cursor()
        created_date = datetime.now(timezone.utc)
        cur.execute(sql, (userid,
                          password,
                          fullname,
                          typeofuser,
                          username,
                          purpose,
                          age,
                          relationship,
                          mobilenumber,
                          gender,
                          birthdate,
                          created_date,
                          emailaddress,
                          photo,
                          patientid,
                          '',
                          '',
                          '',))
        uid = cur.fetchone()[0]
        if uid:
            conn.commit()
            print("Committed --> " + str(uid))
        else:
            ret = -1
            status = "Failed to commit."
    except (Exception, psycopg2.DatabaseError) as error:
        print("DB Error: " + str(error))
        ret = -1
        status = str(error)
    finally:
        if conn is not None:
            conn.close()
        return ret, status
