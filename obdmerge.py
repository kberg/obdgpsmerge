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

def usage():
  print "Usage: ... tbd"

def verbose(text):
  if (verbose):
    print text

# Write methods
#
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
  oconn.commit()

def initializeMergeDb(db):
  conn = sqlite3.connect(db)
  conn.execute('''CREATE TABLE trip (
      tripid INTEGER PRIMARY KEY,
      start REAL,
      end REAL DEFAULT -1)''')

  conn.execute('''CREATE TABLE tripmap (
      tripid INTEGER PRIMARY KEY,
      sourcedb TEXT,
      sourcetripid INTEGER)''')

  return conn

def getFieldsFrom(db):
  pass

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