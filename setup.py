#
# setup.py
# Build the Hbase Client Interface Module
#
from distutils.core import setup, Extension
import os
import sys

setup(name='hbase',
      ext_modules=[
        Extension('hbase',
                  ['hbaseClient.c'],
                  include_dirs = ['./',
                                  '{0}/include'.format(os.environ.get('JAVA_HOME')),
                                  '{0}/include/linux'.format(os.environ.get('JAVA_HOME'))],
                  library_dirs = ['{0}/jre/lib/amd64/server/'.format(os.environ.get('JAVA_HOME'))],
                  libraries = ['jvm'],
                  define_macros=[('PYTHON', sys.version[0])],
                  )
        ]
)
