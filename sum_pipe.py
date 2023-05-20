#!/usr/bin/python3
"""

This is a python program which consumes a non-seekable tar stream on stdin
and emits sha256 sums in Triage format. 

Use example:
tar -c ./ | sum_pipe.py -o tar_sums.out | mbuffer -m2G -P90 -o /dev/nst0

"""

__ID__ = '$Id: sum_pipe.py,v 1.2 2020/05/10 18:48:14 nick Exp nick $'

import argparse
import bz2
import io
import errno
import getopt
import gzip
import logging
import sys
import os
import pprint
import tarfile
import hashlib
import humanize

def GetTriageLine(tarinfo, fileobj, algo='sha256'):
  """ Produce a sha256 sum as a string in Triage format.
  
  Args:
    tarinfo: Tarinfo object from tarfile class.
    fileobj: File like object w/ read function to be summed.
    algo: Hash algorithm to use.

  Returns:
    str:    Sum in Triage format.

            File size, tab, Hash of first 512 bytes, tab, 
            Hash of complete file, tab, file name.
  """
  first_hash = hashlib.new(algo)
  full_hash = hashlib.new(algo)

  c = fileobj.read(512)
  first_hash.update(c)
  while c:
    full_hash.update(c)
    c = fileobj.read(65535)

  return '%s\t%s\t%s\t%s' % (
      tarinfo.size, first_hash.hexdigest(), 
      full_hash.hexdigest(), tarinfo.name)


def FastProcessTarPipe(tar_fh):
  """ Consume a non-seekable tar stream producing no output. 
  For benchmarking. """
  tf = tarfile.open(mode="r|", fileobj=tar_fh, bufsize=65536)
  next_file = tf.next()
  while next_file:
    next_file = tf.next()
  return

def ProcessTarPipe(tar_fh, triage_fd, algo='sha256', delimiter=b'\n'):
  """ Consume a non-seekable tar stream and emit sums in Triage format.

  Args:
    tar_fh:     File like object to consume tar stream from.
    triage_fd:  File object to emit Triage lines.
    algo:       Hashing algorithm to use.
    delimiter:  Record delimiter.

  Returns:
    None
  """
  tf = tarfile.open(
      fileobj=tar_fh, bufsize=65535, mode='r|', ignore_zeros=True,
      debug=1, errorlevel=0)

  next_file = tf.next()

  files_seen = 0
  bytes_seen = 0
  files_skipped = 0

  while next_file:
    # Only produce sums for regular files.
    if next_file.isreg():
      files_seen+=1
      bytes_seen+=next_file.size
      triage_fd.write(
          os.fsencode(GetTriageLine(next_file, tf.extractfile(next_file), algo))
          )
      # Write delimiter after each triage line.
      triage_fd.write(delimiter)
    else:
      files_skipped+=1

    # Set to empty list to clear memory.
    tf.members=[]
    next_file = tf.next()

  if files_skipped > 0:
    sys.stderr.write('Warning: Skipped %s (non-files)\n' % files_skipped)

  sys.stderr.write('Processed %s files %s bytes.\n' % (
    files_seen, humanize.naturalsize(bytes_seen, binary=True))
    )

class IOTee(io.BufferedReader):
  def __init__(self, target_fd=sys.stdout.buffer, *args, **kwargs):
    self.target_fd=target_fd
    super(IOTee, self).__init__(*args, **kwargs)

  def read(self, *args, **kwargs):
    a = super(IOTee, self).read(*args, **kwargs)
    self.target_fd.write(a)
    return a

def ParseArgs():
  parser = argparse.ArgumentParser(description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument("--sink", help="Consume tar output (do not tee).",
      action="store_true")
  parser.add_argument("-0", "--null", help="Produce null-delimited output.",
      action="store_true")
  compress = parser.add_mutually_exclusive_group()
  compress.add_argument("-g", "--gzip", help="Gzip decompress tar file.",
      action="store_true")
  compress.add_argument("-j", "--bzip2", help="Bz2 decompress tar file.",
      action="store_true")
  parser.add_argument("--algo", help="Hashing algorithm to use.",
      choices=hashlib.algorithms_available, default='sha256')
  overwrite = parser.add_mutually_exclusive_group()
  overwrite.add_argument("--overwrite_sum", help="Overwrite sum file.",
      action="store_true")
  overwrite.add_argument("-a", "--append", help="Append to sum file.",
      action="store_true")
  parser.add_argument("--list-available-hashes", 
      help="Print list of hashing algorithms available and exit.", 
      action="store_true")
  parser.add_argument("-o", "--output", help="Where to write sum data.")
  parser.add_argument("-f", "--input", help="Input tar file.", default='-')
  args = parser.parse_args()

  if args.list_available_hashes:
    print('Passed --list-available-hashes just printing hash list.')
    pprint.pprint(hashlib.algorithms_available)
    sys.exit(0)

  if not args.output:
    parser.error('--output required.')

  return args


def Main():
  args=ParseArgs()

  if args.input=='-' or args.input=='/dev/stdin':
    input=sys.stdin.buffer
  else:
    input=open(args.input, 'br')

  if args.sink:
    input_fd=input
  else:
    input_fd=IOTee(raw=input, target_fd=sys.stdout.buffer)

  if args.gzip:
    input_fd=gzip.GzipFile(mode='rb', fileobj=input_fd)

  if args.bzip2:
    input_fd=bz2.BZ2File(mode='rb', filename=input_fd)

  if args.null:
    delimiter=b'\0'
  else:
    delimiter=b'\n'

  print(args.output)

  if args.output=='-' or args.output=='/dev/stdout':
    triage_fd=sys.stdout.buffer
  else:
    if os.path.exists(args.output):
      if not args.overwrite_sum and not args.append:
        #('Specified hash output file exists and neither overwrite_sum or ',
        #'append are set')
        raise FileExistsError(errno.EEXIST, 
            os.strerror(errno.EEXIST), args.output)

      if args.append:
        mode='ab'
      elif args.overwrite_sum:
        mode='wb'
      else:
        # This should never happen.
        raise RuntimeException('neither overwrite_sum or append are set')
    else:
      mode='wb'

    triage_fd=open(args.output, mode)

  ProcessTarPipe(input_fd, triage_fd, algo=args.algo, delimiter=delimiter)


if __name__ == '__main__':
  Main()
