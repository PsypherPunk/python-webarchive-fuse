#!/usr/bin/env python

import sys
import logging
from fuse import FUSE
from webarchivefuse import WarcFileSystem

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "webarchivefuse" )

if __name__ == "__main__":
	if len( sys.argv ) != 3:
		print( "usage: %s <warc> <mountpoint>" % sys.argv[ 0 ] )
		sys.exit( 1 )

	fuse = FUSE( WarcFileSystem( sys.argv[ 1 ] ), sys.argv[ 2 ], foreground=True )

