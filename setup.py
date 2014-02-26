from distutils.core import setup

setup(
	name="python-webarchivefuse",
	version="0.0.1",
	author="PsypherPunk",
	author_email="psypherpunk@gmail.com",
	packages=[ "webarchivefuse" ],
	license="LICENSE.txt",
	description="FUSE library for (W)ARC files.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"treelib",
	],
)
