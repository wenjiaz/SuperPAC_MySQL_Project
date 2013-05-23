import MySQLdb
import datetime


from urllib2 import urlopen
from StringIO import StringIO
import zipfile
#http://forms.irs.gov/887x/PolOrgsFullData.zip



headList=['form_id', 'org', 'EIN', 'name', 'add1', 'add2', 'city', 'state', 'zip', 'zipExt', 'employer', 'amount', 'occupation', 'aggTotal', 'contDate']
intList=['form_id', 'EIN', 'zip', 'zipExt', 'amount', 'aggTotal', 'contDate']
strList=['org', 'nameLast', 'nameFirst', 'compName', 'add1', 'add2', 'city', 'state', 'employer', 'occupation']
fullList=['form_id', 'org', 'EIN', 'nameLast', 'nameFirst', 'compName','add1', 'add2', 'city', 'state', 'zip', 'zipExt', 'employer', 'amount', 'occupation', 'aggTotal', 'contDate', 'present']

columnsString=",".join(fullList)

def addToLog(text):
    file=open("irs_log2", "a")
    file.write("%s at %s\n" %(text,    datetime.datetime.now()))
    file.close()
    
def initLog(text):
    file=open("irs_log2", "w")
    file.write("%s at %s\n" %(text,    datetime.datetime.now()))
    file.close()
    
#----------Start of additional programs---------------------------#
def downloadFile():
    try:
        target = urlopen("http://forms.irs.gov/887x/PolOrgsFullData.zip")
    except IOError, e:
        addToLog("Can't retrieve dataset: 0")
        print "Can't retrieve dataset"
        return 0
    
    try:
        zippy=zipfile.ZipFile(StringIO(target.read()))
        return zippy
        
    except zipfile.error,e:
        addToLog("Can't read dataset:1")
        print "Can't read dataset"
        return 0
        

def insertLine(aDict):
    query="""REPLACE INTO super_pac (form_id, org, EIN, nameLast, nameFirst, compName,add1, add2, city, state, zip, zipExt, employer, amount, occupation, aggTotal, contDate, present) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s) """ 

    cursor.execute(query, ([aDict[key] for key in fullList]))

    
def processName(name, flag):
    #list of stuff to watch out for. If something similar to 'NA' is entered in "occupation" field, it means that entity is a company.
    suffixList=['.jr', 'jr.', 'sr.', 'iii', 'ii', 'i', '.sr']
    OccList=['n/a', 'n\a', 'na', 'none']

    if flag.lower() in OccList:
        firstName=None;
        lastName=None;
        compName=name;
        occ=None
        
    else:
        compName=None
        name=name.split();
        firstName=name[0]
        occ=flag

        if name[-1].lower() in suffixList:
            lastName=name[-2] + name[-1]
        else:
            lastName=name[-1]
            
    return lastName, firstName, compName, occ;



def parseFile(zippy):
    lineCount=0
    x=len(headList)
    try:
        for line in zippy.open('FullDataFile.txt'):
            if line[0]=='A':
                lineCount+=1
                foo=line.split('|')[2:17]
                
                if len(foo)==x:
                    lineDict={ }

                    # put input and header together so it pre-insert data line turns something like this: ('nameFirst': 'Jon', 'nameLast': 'Bruner', 'company': 'Forbes Media', 'city': 'New York' etc..)
                    lineDict=dict(zip(headList, foo))
                    name=lineDict['name']
                    
                    tmp=lineDict['occupation']
                    #use processName to break the 'name' input into first, last
                    lineDict['nameLast'], lineDict['nameFirst'], lineDict['compName'], lineDict['occupation']=processName(name, tmp) 

                    # if value for the category is empty string, put in Null. else, we have to make it " ' string' " so that when we insert into mysql it will preserve string type.
                    for key in lineDict:
                        value=lineDict[key]
                        if value==" " or value=="":
                            lineDict[key]=None
                            
                    #'present' field tells us that the entry is in the IRS data. This is so when we update, anything that has present=0 let us know that IRS has dropped the entry, and we delete it as well
                    lineDict['present']="1"
                    insertLine(lineDict)
                    
                    # this is for logging + commit purposes. 
                    if lineCount % 10000==0:
                        addToLog("Inserted Line %d" % lineCount)
                        conn.commit();
                else:
                    addToLog("Skipped line %d" % lineCount)
    except MySQLdb.Error, e:
        raise
        return 0

    else:
        return 1
#-----------------------------End of additional programs-------------------------------#


initLog("Starting program")

addToLog("connecting to MySQL")
conn=MySQLdb.connect(host="localhost", user="localuser", passwd="localpass", db="FEC")
cursor=conn.cursor()

#Fields are : 
#Form_id (the unique key for the IRS data), Organization, EIN (some tax number), nameLast, nameFirst, compName (null if indiv, name if company), address line1, address line 2, city, state, zip, zip-extension, employer, amount, occupation, aggregate total, contribution date
cursor.execute("""CREATE TABLE IF NOT EXISTS super_pac ( form_id INT PRIMARY KEY, org VARCHAR(100), EIN INT, nameLast VARCHAR(40), nameFirst VARCHAR(40), compName VARCHAR(60), add1 VARCHAR(40), add2 VARCHAR(40), city VARCHAR(25), state VARCHAR(4), zip INT, zipExt INT, employer VARCHAR(90), amount INT, occupation VARCHAR(70), aggTotal INT, contDate INT, present TINYINT(1))""")
cursor.execute("""UPDATE super_pac SET present=0 ;""")

addToLog("Downloading files")
file=downloadFile()
if file:
    addToLog("Download successful. Beginning parse")
    result=parseFile(file)
    
    if result:
        addToLog("Parse completed succesfully")
        cursor.execute("""DELETE FROM super_pac WHERE present=0 ;""")

else:
    addToLog("Download failed")
    
cursor.close()
conn.commit()
conn.close()

addToLog("Program finished")