#!/usr/local/bin/python2.7

# Python 2.6 script for merging multiple odbgps databases.
# Author: Robert Konigsberg
# December 27, 2010

# sqlite reference: http://docs.python.org/library/sqlite3.html

# Assume tables are ecu, gps, obd, trip
# Currently disregarding ecu and gps.
#
# Trips will be given new numbers arbitrarily, someone might want to
# figure out trip numbers based on timestamps, but I'm not doing that
# yet.

import getopt
import sqlite3
import sys

tripcount = 0
currentdb = None
oconn = None

# (olddb, oldtripid) -> newtripid
tripmap = {}

# All fields (except default ones), disambiguated
allfields = set()

def usage():
  print "Usage: ... tbd"

def verbose(text):
  if (verbose):
    print text

def addToTripMap(currentdb, tripid, newTripId):
  global tripmap
  tripmap[(currentdb, tripid)] = newTripId

def getNewTripId(currentdb, tripid):
  global tripmap
  return tripmap[(currentdb, tripid)]

# Write methods
#

# Write information about a trip to the output database, and also create
# an in-memory cross-reference
def writeTrip(tripid, start, end):
  global oconn
  global currentdb
  global tripcount
  tripcount = tripcount + 1

  oconn.execute("insert into trip(tripid, start, end) values (?,?,?)", 
    (tripcount, start, end))
  oconn.execute(
    "insert into tripmap(tripid, sourcedb, sourcetripid) values(?, ?, ?)",
    (tripcount, currentdb, tripid))

  addToTripMap(currentdb, tripid, tripcount)

  oconn.commit()

# Initialize the output database.
def initializeMergeDb(db):
  conn = sqlite3.connect(db)
  conn.execute('''CREATE TABLE IF NOT EXISTS trip(
      tripid INTEGER PRIMARY KEY,
      start REAL,
      end REAL DEFAULT -1)''')

  conn.execute('''CREATE TABLE IF NOT EXISTS tripmap(
      tripid INTEGER PRIMARY KEY,
      sourcedb TEXT,
      sourcetripid INTEGER)''')
      
  conn.execute('''CREATE TABLE IF NOT EXISTS obd(
    time REAL,
    trip INTEGER,
    ecu INTEGER DEFAULT 0)''')

  conn.execute("CREATE INDEX IDX_OBDTIME ON obd (time)")
  conn.execute("CREATE INDEX IDX_OBDTRIP ON obd (trip)")

  # TODO(konigsberg): Add indices to obd

  return conn

def addObdColumn(column):
  global allfields
  global oconn

  # Ignore default columns
  if column in set(("time", "trip", "ecu")):
    return

  # Don't duplicate fields
  if column in allfields:
    return

  verbose("Adding columnn: %s" % column)

  oconn.execute("ALTER TABLE obd ADD %s REAL" % column)
  allfields = allfields | set([column])

# Initial pass at database scanning, reads all the database fields.
def getFieldsFrom(conn):
  global currentdb
  cur = conn.cursor()
  cur.execute('select * from obd')
  row = cur.fetchone()
  for column in row.keys():
    addObdColumn(column)

  cur.close()

# Initialize the list of trips. This 
def getTripsFrom(conn):
  global tripcount
  cur = conn.cursor()
  cur.execute('select * from trip')
  for row in cur:
    writeTrip(row["tripid"], row["start"], row["end"])
  cur.close()

def readDb(dbs):
  global currentdb
  for db in dbs:
    currentdb = db
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    verbose("Reading metadata from %s" % db)
    fields = getFieldsFrom(conn)
    trips = getTripsFrom(conn)
    conn.close()

def main(argv):
  global oconn
  global allfields
  try:
    output="merged.db"
    opts, args = getopt.getopt(argv, "vo:", ["verbose", "output="])
    # Finish optarg processing http://www.faqs.org/docs/diveintopython/kgp_commandline.html
    oconn = initializeMergeDb(output)
    readDb(args)

    oconn.close()

  except getopt.GetoptError:
    usage()
    sys.exit(2)

if __name__ == "__main__":
  main(sys.argv[1:])