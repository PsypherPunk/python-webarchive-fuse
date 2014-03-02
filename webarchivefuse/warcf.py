#!/usr/bin/env python

import os
import re
import sys
import time
import treelib
import logging
import progressbar
from errno import EPERM, ENOENT, ENODATA
from treelib import Node, Tree
from dateutil.parser import parse
from hanzo.warctools import WarcRecord
from hanzo.warctools.stream import open_record_stream
from stat import S_IFDIR, S_IFLNK, S_IFREG
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

LOGGING_FORMAT="[%(asctime)s] %(levelname)s: %(message)s"
logging.basicConfig( format=LOGGING_FORMAT, level=logging.DEBUG )
logger = logging.getLogger( "webarchivefuse" )

class WarcRecordNode( Node ):
	"""Node with a WarcRecord and offset."""
	def __init__( self, record, offset, tag=None, identifier=None, expanded=True ):
		Node.__init__( self, tag=tag, identifier=identifier, expanded=expanded )
		self.record = record
		self.offset = offset
		self.xattrs = {}
		for k, v in record.headers:
			self.xattrs[ k ] = v

class WarcFileSystem( LoggingMixIn, Operations ):
	"""Filesystem built on a WARC's URI paths."""
	def __init__( self, warc ):
		self.warc = warc
		logger.debug( "Mounting %s" % self.warc )
		self.fh = WarcRecord.open_archive( warc, gzip="auto", mode="rb" )
		self.tree = Tree()
		self._get_records()

	def _get_records( self ):
		"""Parses a WARC, building a hierarchical tree."""
		statinfo = os.stat( self.warc )
		self.gid = statinfo.st_gid
		self.uid = statinfo.st_uid
		self.tree.create_node( self.warc, "/" )
		self.records = {}
		bar = progressbar.ProgressBar( maxval=statinfo.st_size, widgets=[ progressbar.Bar( "=", "[", "]"), " ", progressbar.Percentage() ] )
		bar.start()
		for( offset, record, errors ) in self.fh.read_records( limit=None ):
			if record is not None and record.type != WarcRecord.WARCINFO:
				logger.debug( "%s:%s" % ( record.type, record.url ) )
				parent = "/"
				nodes = [ record.type ] + re.split( "/+", record.url )
				for e in nodes:
					identifier = "/".join( [ parent, e ] )
					if not self.tree.contains( identifier ):
						node = WarcRecordNode( record, offset, tag=e, identifier=identifier )
						self.tree.add_node( node, parent=parent )
					parent = identifier
				self.records[ record.url ] = ( offset, record )
				bar.update( offset )
		bar.finish()
		logger.debug( self.tree.show() )

#	def access( self, path, amode ):
#		logger.debug( path )
#		raise FuseOSError( EPERM )

	def chmod( self, path, mode ):
		raise FuseOSError( EPERM )

	def chown( self, path, uid, gid ):
		raise FuseOSError( EPERM )

	def create( self, path, mode ):
		raise FuseOSError( EPERM )

	def destroy( self, path ):
		self.fh.close()

#	def flush( self, path, fh ):
#		raise FuseOSError( EPERM )

	def fsync( self, path, datasync, fh ):
		raise FuseOSError( EPERM )
		
	def fsyncdir( self, path, datasync, fh ):
		raise FuseOSError( EPERM )

	def getattr( self, path, fh=None ):
		"""Returns stat info for a path in the tree."""
		logger.debug( path )
		if path == "/":
			stat = os.stat( self.warc )
			return dict( [
				( "st_mode", ( S_IFDIR | 0444 ) ),
				( "st_ino", stat.st_ino ),
				( "st_dev", stat.st_dev ),
				( "st_nlink", stat.st_nlink ),
				( "st_uid", stat.st_uid ),
				( "st_gid", stat.st_gid ),
				( "st_size", stat.st_size ),
				( "st_ctime", stat.st_ctime ),
				( "st_mtime", stat.st_mtime ),
				( "st_atime", stat.st_atime )
			] )
		else:
			return self.name_to_attrs( "/%s" % path )

	def getxattr( self, path, name, position=0 ):
		"""Returns the value for an extended attribute."""
		if path != "/":
			path = "/%s" % path

		node = self.tree.get_node( path )
		if node is None:
			raise FuseOSError( ENOENT )

		try:
			return node.xattrs[ name ]
		except KeyError:
			raise FuseOSError( ENODATA )

	def init( self, path ):
		pass

	def link( self, target, source ):
		raise FuseOSError( EPERM )

	def listxattr( self, path ):
		"""Returns a list of extended attribute names."""
		if path != "/":
			path = "/%s" % path

		node = self.tree.get_node( path )
		if node is None:
			raise FuseOSError( ENOENT )
		return node.xattrs.keys()

	def mkdir( self, path, mode ):
		raise FuseOSError( EPERM )

	def mknod( self, path, mode, dev ):
		raise FuseOSError( EPERM )

	def open( self, path, flags ):
		"""Should return numeric filehandle; returns file offset for convenience."""
		if path != "/":
			path = "/%s" % path

		node = self.tree.get_node( path )
		if node is None:
			raise FuseOSError( ENOENT )

		return node.offset

#	def opendir( self, path ):
#		raise FuseOSError( EPERM )

	def read( self, path, size, offset, fh ):
		"""Reads 'size' data from 'path', starting at 'offset'."""
		logger.debug( "read %s from %s at %s " % ( size, path, offset ) )

		if path != "/":
			path = "/%s" % path

		node = self.tree.get_node( path )
		if node is None:
			raise FuseOSError( ENOENT )

		mime, data = node.record.content
		end = offset + size
		return data[ offset:end ]

	def name_to_attrs( self, name ):
		"""Retrieves attrs for a path name."""
		logger.debug( name )
		node = self.tree.get_node( name )
		if node is None:
			raise FuseOSError( ENOENT )

		if node.is_leaf():
			st_mode = ( S_IFREG | 0444 )
			size = node.record.content_length
			timestamp = time.mktime( parse( node.record.date ).timetuple() )
		else:
			st_mode = ( S_IFDIR | 0555 )
			size = 0
			timestamp = time.time()
		return dict( [
			( "st_mode", st_mode ),
			( "st_ino", 0 ),
			( "st_dev", 0 ),
			( "st_nlink", 0 ),
			( "st_uid", self.uid ),
			( "st_gid", self.gid ),
			( "st_size", size ), 
			( "st_ctime", timestamp ),
			( "st_mtime", timestamp ),
			( "st_atime", timestamp )
		] )

	def readdir( self, path, fh ):
		"""Returns a tuple of all files in path."""
		logger.debug( path )
		if path != "/":
			path = "/%s" % path
		if self.tree.contains( path ):
			names = [ ".", ".." ]
			for node in self.tree.all_nodes():
				if self.tree.parent( node.identifier ) is not None and self.tree.parent( node.identifier ).identifier == path:
					names.append( ( node.tag, self.name_to_attrs( node.identifier ), 0  ) )
			return names
		else:
			raise FuseOSError( ENOENT )

	def readlink( self, path ):
		raise FuseOSError( EPERM )

#	def release( self, path, fh ):
#		raise FuseOSError( EPERM )

#	def releasedir( self, path, fh ):
#		raise FuseOSError( EPERM )

	def removexattr( self, path, name ):
		raise FuseOSError( EPERM )

	def rename( self, old, new ):
		raise FuseOSError( EPERM )

	def rmdir( self, path ):
		raise FuseOSError( EPERM )

	def setxattr( self, path, name, value, options, position=0 ):
		raise FuseOSError( EPERM )

	def statfs( self, path ):
		raise FuseOSError( EPERM )

	def symlink( self, target, source ):
		raise FuseOSError( EPERM )

	def truncate( self, path, length, fh=None ):
		raise FuseOSError( EPERM )

	def unlink( self, path ):
		raise FuseOSError( EPERM )

	def utimens( self, path, times=None ):
		raise FuseOSError( EPERM )

	def write( self, path, data, offset, fh ):
		raise FuseOSError( EPERM )

