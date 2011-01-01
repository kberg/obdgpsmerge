#!/usr/local/bin/python2.7

# Python 2.6 script for merging multiple odbgps databases.
# Author: Robert Konigsberg
# December 27, 2010

# sqlite reference: http://docs.python.org/library/sqlite3.html

# Assume tables are ecu, gps, obd, trip
#
# Trips will be given new numbers arbitrarily, someone might want to
# figure out trip numbers based on timestamps, but I'm not doing that
# yet.
#
# Adds an additional table called tripmap which maps trip ids back to
# source data

# TODO: Store GPS and ECU data.

import getopt
import sqlite3
import sys

tripcount = 0
currentdb = None
oconn = None
rowcount = 0

# oldtripid -> newtripid
tripmap = {}

# (ecu, vin) -> ecu id
ecumap = {}

# All fields deduped. Initialized with the default fields.
allfields = set(["time", "trip", "ecu"])

def usage():
  print "Usage: ... tbd"

def addToTripMap(oldTripId, newTripId):
  global tripmap
  tripmap[oldTripId] = newTripId

def getNewTripId(oldTripId):
  global tripmap
  return tripmap[oldTripId]

def setNewDatabse(db):
  global currentdb
  global tripmap

  currentdb = db;
  tripmap = {}

# Write methods
#

# Write information about a trip to the output database, and also create
# an in-memory cross-reference
def writeTrip(row):
  tripId = row["tripid"]

  global oconn
  global currentdb
  global tripcount
  tripcount = tripcount + 1

  oconn.execute("insert into trip(tripid, start, end) values (?,?,?)", 
    (tripcount, row["start"], row["end"]))
  oconn.execute(
    "insert into tripmap(tripid, sourcedb, sourcetripid) values(?, ?, ?)",
    (tripcount, currentdb, tripId))

  addToTripMap(tripId, tripcount)

  oconn.commit()

# Write information about an obd row to the output database. Translate the trip
# id and ecu id
def writeObd(row):
  global oconn
  global currentdb
  global rowcount

  # Add columns, could be done just the first time, optimize later.
  for column in row.keys():
    addObdColumn(column)

  # I'm sure there's smarter python than this. Whatever.
  # This just wraps they keys and values into two lists, keys and values
  # match by index number.
  keys=[]
  values=[]
  for key in row.keys():
    keys.append(key)
    val = row[key]
    if (key == "trip"):
      val = getNewTripId(val)

    # ECU data not well supported, don't write it.
    # TODO(konigsberg): support it.
    if (key == "ecu"):
      val = None
    values.append(row[key])

  params = ",".join(["?"] * len(row))
  stmt = "insert into obd (%s) values (%s)" % (",".join(keys), params)

  oconn.execute(stmt, values)

  # Make 1000 a command-line option --commit_count?
  rowcount = rowcount + 1
  if rowcount % 1000 == 0:
    print rowcount
    oconn.commit()

def writeEcu(row):
  #row["ecuid"], row["vin"], row["ecu"], row["ecudesc"]
  pass

# Write information about a row to the output database. Translate the trip
# id.
def writeGps():
  global oconn
  global currentdb
  global rowcount

  newTripId = getNewTripId(val)

  stmt = "insert into gps (lat, lon, alt, speed, course, gpstime, time, trip) values (?,?,?,?,?,?,?,?)"

  oconn.execute(stmt, row["lat"], row["lon"], row["alt"], row["speed"],
      row["course"], row["gpstime"], row["time"], newTripId)

  # Make 1000 a command-line option --commit_count?
  rowcount = rowcount + 1
  if rowcount % 1000 == 0:
    print rowcount
    oconn.commit()

# Initialize the output database.
def initializeOutputDb(conn):
  conn.execute('''CREATE TABLE trip(
      tripid INTEGER PRIMARY KEY,
      start REAL,
      end REAL DEFAULT -1)''')

  conn.execute('''CREATE TABLE tripmap(
      tripid INTEGER PRIMARY KEY,
      sourcedb TEXT,
      sourcetripid INTEGER)''')
      
  conn.execute('''CREATE TABLE obd(
    time REAL,
    trip INTEGER,
    ecu INTEGER DEFAULT 0)''')

  conn.execute("CREATE INDEX IDX_OBDTIME ON obd (time)")
  conn.execute("CREATE INDEX IDX_OBDTRIP ON obd (trip)")

  conn.execute('''CREATE TABLE gps(
    lat REAL,
    lon REAL,
    alt REAL,
    speed REAL,
    course REAL,
    gpstime REAL,
    time REAL,
    trip INTEGER)''')
  conn.execute("CREATE INDEX IDX_GPSTIME ON gps (time)")
  conn.execute("CREATE INDEX IDX_GPSTRIP ON gps (trip)")

  conn.execute('''CREATE TABLE ecu(
    ecuid INTEGER PRIMARY KEY,
    vin TEXT,
    ecu INTEGER,
    ecudesc TEXT)''')

  conn.execute("CREATE UNIQUE INDEX IDX_VINECU ON ecu (vin,ecu)")

  conn.execute('''CREATE TABLE ecumap(
      ecuid INTEGER PRIMARY KEY,
      sourcedb TEXT,
      sourceecuid INTEGER)''')

  return conn

def addObdColumn(column):
  global allfields
  global oconn

  # Ignore default columns and fields already added.
  if column in allfields:
    return

  print "Adding columnn: %s" % column

  oconn.execute("ALTER TABLE obd ADD %s REAL" % column)
  allfields = allfields | set([column])

# Read the trips table
def processTrips(conn):
  cur = conn.cursor()
  cur.execute('select * from trip')
  for row in cur:
    writeTrip(row)
  cur.close()

# Read the ECU table
def processEcu(conn):
  cur = conn.cursor()
  cur.execute('select * from ecu')
  for row in cur:
    writeEcu(row)
  cur.close()

# Read the GPS table
def processGps(conn):
  cur = conn.cursor()
  cur.execute('select * from gps')
  for row in cur:
    writeGps(row)
  cur.close()

# Read and proecss the obd table
def processObd(conn):
  global currentdb
  cur = conn.cursor()
  cur.execute('select * from obd')
  for row in cur:
    writeObd(row)
  cur.close()

def processDatabase(db):
  conn = sqlite3.connect(db)
  conn.row_factory = sqlite3.Row
  print "Reading from %s" % db
  processTrips(conn)
  processEcu(conn)
  processObd(conn)
  processGps(conn)
  
  conn.close()

def main(argv):
  global oconn
  global rowcount
  try:
    output="merged.db"
    opts, args = getopt.getopt(argv, "o:", ["output="])
    # Finish optarg processing http://www.faqs.org/docs/diveintopython/kgp_commandline.html
    oconn = sqlite3.connect(output)
    initializeOutputDb(oconn)
    for db in args:
      setNewDatabse(db)
      processDatabase(db)
      oconn.commit()
    oconn.close()
    print "Finished processing %d rows" % rowcount

  except getopt.GetoptError:
    usage()
    sys.exit(2)

if __name__ == "__main__":
  main(sys.argv[1:])