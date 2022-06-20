'''Design a database with 3 pairs of Many to Many relationships.'''

import sqlite3

# create sqlite database
con = sqlite3.connect('roster.sqlite')
cursor = con.cursor()

# create 5 tables
cursor.executescript('''
DROP TABLE IF EXISTS Student;
DROP TABLE IF EXISTS Class;
DROP TABLE IF EXISTS Professor;
DROP TABLE IF EXISTS SandC;
DROP TABLE IF EXISTS CandP;

CREATE TABLE Student (
    id integer primary key autoincrement not null unique,
    name text unique
);
CREATE TABLE Class (
    id integer primary key autoincrement not null unique,
    courseTitle text unique
);
CREATE TABLE Professor (
    id integer primary key autoincrement not null unique,
    name text unique
);
CREATE TABLE SandC (
    sid integer,
    cid integer,
    primary key (sid, cid)
);
CREATE TABLE CandP (
    cid integer,
    pid integer,
    primary key (cid, pid)
);
''')

import json
# read from json file
data = open('code3/code3/roster/roster_data_sample.json').read() # type is str
# conver str to json
jsondata = json.loads(data)

# insert all courses into Class table
for entry in jsondata:
    courseName = entry[1]
    cursor.execute(
        '''INSERT OR IGNORE INTO Class (courseTitle) values (?)''',(courseName,)
    )

# read professor.csv file
import csv

pFile = open('code3/code3/roster/professor.csv')
csvFile = csv.reader(pFile, delimiter=',') #  iterator, can't use indexing
csvList = list(csvFile)
for row in csvList:
    # print(row) # output : ['Alfred', 'Clark'] ['Amelia', 'Richards'] ['Haris', 'Ross']
    fullname = f'{row[0]} {row[1]}'
    # print(name)
    cursor.execute(
        'INSERT INTO Professor (name) values (?);',(fullname,)
    )

# populate Student table from the json file
# print(jsondata) #[['Charley', 'si110', 1], ['Mea', 'si110', 0], ...]
for entry in jsondata:
    studentName = entry[0]
    # use IGNORE to warrant UNIQUE constraint
    cursor.execute('''
        INSERT OR IGNORE INTO Student (name) values (?)        
    ''',(studentName,))

# populate SandC table
for entry in jsondata:
    studentName = entry[0]
    courseName = entry[1]

    # get sid from Student table
    cursor.execute('''
    SELECT id FROM Student WHERE name = (?)
    ''',(studentName,))
    studentId = cursor.fetchone()[0]

    # get cid from Class table
    cursor.execute('''
    SELECT id FROM Class WHERE courseTitle = (?)
    ''', (courseName,))
    courseID = cursor.fetchone()[0]

    # insert pair of ids into the table
    cursor.execute('''
    INSERT INTO SandC (sid, cid) values (?,?)
    ''',(studentId, courseID))

# randomly create CandP table by assign 1-3 professors to each course randomly

import  random

# helper function for assigning professors to this course
def get_professor_ids():
    # how many professors for this course
    num = random.randint(1,3) # output randomly 1,2,3
    
    cursor.execute('select id from Professor')
    professorIDs = [idTuple[0] for idTuple in cursor.fetchall()]
    p_ids = random.choices(professorIDs,k=num)
    return p_ids

# collect all course ids into a list
cursor.execute('SELECT id FROM Class')
courseIDs = [courseID[0] for courseID in cursor.fetchall()]

# assign a random number of professors to each course
for courseID in courseIDs:
    professors_ids = get_professor_ids()
    for p_id in professors_ids:
        cursor.execute('INSERT OR IGNORE INTO CandP (cid, pid) values (?,?)', (courseID, p_id))


# use set() to get list of professors without class to teach, assign one class to him/her
# 1. get all professors' ids into a list from CandP then from Professor table
cursor.execute('SELECT pid FROM CandP;')
pids = [id[0] for id in cursor.fetchall()]

cursor.execute('SELECT id FROM Professor')
pro_ids = [id[0] for id in cursor.fetchall()]

# compare the two list to find difference using set()'s '-' operator
no_class_pids = list(set(pro_ids)-set(pids))
# print(no_class_pids) # [2, 4, 7, 10, 12, 16, 18, 19]

# keep on adding new rows into CandP table with a professor and a randomly assigned class id
for pid in no_class_pids:
    courseId = random.choice(courseIDs)
    cursor.execute('INSERT INTO CandP (cid, pid) values (?,?)', (courseId, pid))


# create a table SCP to connect all tables
# copy SandC, add one column 'pid',
cursor.execute('create table SCP AS select * from SandC')
cursor.execute('alter table SCP add pid;')


# choose random one id to insert into the cell of 'pids' using CandP 

# STEP 1: A helper function to randomly choose a pid from cid
def get_one_professor(courseid):
    cursor.execute('select pid from CandP Where cid=?',(courseid,))
    pid = random.choice([id[0] for id in cursor.fetchall()])
    return pid

# STEP 2: get cid and sid of each row, update SCP with the newly assigned pid
cursor.execute('select cid,sid from SCP')
print(cursor.fetchall())
for theCID, theSID in cursor.fetchall():
    pid = get_one_professor(theCID)
    cursor.execute('''
    UPDATE SCP SET pid = (?)
    WHERE cid = (?) AND sid = (?)
    ''', (pid, theCID, theSID))


# join tables to show student, class and professor
cursor.execute('''
SELECT Student.name, Class.courseTitle, Professor.name 
From Student JOIN Class JOIN Professor JOIN SCP
ON Student.id = SCP.sid
AND Class.id = SCP.cid
AND Professor.id = SCP.pid
ORDER By Student.name, Class.id, Professor.name;
''')

print(cursor.fetchall())

con.commit()
con.close()