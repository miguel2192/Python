#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

DESCRIPTION

	Handler treating MongoDB with large size of data (e.g. image, numeric array, etc).
	As the limit of 16 MB per document in MongoDB, the data is stored in the database with GridFS.
	:ref:https://docs.mongodb.com/manual/core/gridfs/

AUTHOR

	Sungsoo Ha (hasungsoo at gmail dot com)
	Miguel Rodriguez miguelrdrgz47@gmail.com extended functionality

LICENSE

	Public Domain

"""
from pymongo import MongoClient
import gridfs
from PIL import Image
import io
import os
import numpy as np

# setup mongo
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017

# connect to the database & get a gridfs handle
mongo_con = MongoClient(MONGODB_HOST, MONGODB_PORT)
mongo_db = mongo_con["scientific_data"]
grid_fs = gridfs.GridFS(mongo_db, "fs")


def file_add(content_type, directory_name):
	"""
	Simpifies gathering the directory info, reads and adds the necessary files to the DB
	:param content_type: content type as a string
	:param directory_name: Directory name or folder name
	"""


	if content_type == 'thumbnail':

		directory = os.getcwd()+ '/' + directory_name + '/'

		for filename in os.listdir(directory):

			if filename.endswith(".jpg"):

				add_data(filename, content_type)

			else:
				print('Wrong file extention!')

	elif content_type == 'npy':

		directory = os.getcwd()+ '/' + directory_name + '/'

		for filename in os.listdir(directory):

			if filename.endswith(".npy"):

				add_data(filename, content_type)

			else:
				print('Wrong file extention!')

	elif content_type == 'image':

		directory = os.getcwd()+ '/' + directory_name + '/'

		for filename in os.listdir(directory):

			if filename.endswith(".jpg" or '.png'):

				add_data(filename, content_type)
			else:
				print('Wrong file extention!')

	else:
		print('Wrong content_type or directory_name')




def add_data(filename, content_type):
	"""
	Add data into database. Each data is differentiated with the combination of filename and content type
	:param filename: full path file name
	:param content_type: content type
	"""

	# gridfs filename
	# gridfs_filename = get_filename(filename)

	if grid_fs.exists(filename=filename, contentType=content_type):
		print("mongo: file {0} with {1} type exist!".format(filename, content_type))
	else:
		print('mongo: file {0} with {1} type does not exists'.format(filename, content_type))


	if filename.endswith(".jpg"):
		directory = os.getcwd() + '/thumbnails/'
	elif filename.endswith(".npy"):
		directory = os.getcwd() + '/npy/'
	else:
		print("Invalid extention or not currently supported!")


	# read image as byte string
	data = open(directory + filename, "rb").read()

	# insert the resource into gridfs
	_id = grid_fs.put(data, contentType=content_type, filename=filename)
	print("mongo: created new gridfs file {0} with id {1}".format(filename, _id))


def get_specific(filename, content_type):
	"""
	Get data from database.
	:param filename: full path file name
	:param content_type: content type
	:return: converted data according to content type
	"""

	# gridfs filename
	# gridfs_filename = get_filename(filename)

	if not grid_fs.exists(filename=filename, contentType=content_type):
		raise Exception("mongo: file {0} with {1} type does not exist!".format(filename, content_type))


	grid_cursor = grid_fs.get_last_version(filename)

	data = grid_cursor.read()

	if content_type in ("image", "thumbnail"):
		out_data = Image.open(io.BytesIO(data))
		print('file was found!')
	elif content_type in "npy":
		out_data = np.load(io.BytesIO(data))
		print('file was found!')
	else:
		out_data = data
		print('file was not found!')

	return out_data

def get_all(content_type):
	"""
	Get data from database base on the specific content type.
	:param content_type: content type
	:return: all data for corresponding file content type
	"""

	if grid_fs.exists(contentType=content_type):
		for grid_cursor in grid_fs.find({"contentType": content_type},
							no_cursor_timeout=True):
			data = grid_cursor.read()

			if content_type == 'thumbnail':
				out_data = Image.open(io.BytesIO(data))


				print('Saving thumbnail(s)...')
				out_data.save(grid_cursor.filename)

			elif content_type == 'image':
				out_data = Image.open(io.BytesIO(data))


				print('Saving image(s)...')
				out_data.save(grid_cursor.filename)
			elif content_type == 'npy':

				print('Saving numpy file(s)...')
				out_data = np.load(io.BytesIO(data))
				np.save(grid_cursor.filename, out_data)

			else:
				print("Sorry! Content type does not exists in the database.")
	else:
		print('Files are unreachable because they do not exist in the database!')





def del_specific(filename, content_type):
	"""
	Delete data from the database if and only if there is a matched item
	:param filename: full path file name
	:param content_type: content type
	"""

	# gridfs_filename = get_filename(filename)

	if grid_fs.exists(filename=filename, contentType=content_type):
		grid_cursor = grid_fs.get_last_version(filename)
		grid_fs.delete(grid_cursor._id)
		print(filename + " was deleted from the database.")

def del_all(content_type):
	"""
	Deletes all data specific to the content type provided
	:param content: content_type
	"""

	# gridfs_filename = get_filename(filename)
	if grid_fs.exists(contentType=content_type):

		for grid_cursor in grid_fs.find({"contentType": content_type},
							no_cursor_timeout=True):

			if content_type == 'thumbnail':

				grid_fs.delete(grid_cursor._id)

				print('Deleting thumbnail(s)...')

			elif content_type == 'image':
				grid_fs.delete(grid_cursor._id)

				print('Deleting image(s)...')

			elif content_type == 'npy':
				grid_fs.delete(grid_cursor._id)

				print('Deleting numpy file(s)...')

			else:
				print("Sorry! Content type does not exists in the database.")
	else:
		print('The file content type does not exist in the database!')


def find_amount(content_type):

	count = 0

	for grid_out in grid_fs.find({"contentType": content_type},
						no_cursor_timeout=True):
			count += 1

	return 'There are ' + str(count) + ' files referencing the content type '+ content_type


def rm_all():
	"""
	Deletes all data specific to the content type provided
	:param content: content_type
	"""

	for grid_cursor in grid_fs.find({},
						no_cursor_timeout=True):

		grid_fs.delete(grid_cursor._id)
		print('Deleting file(s)...')


def main():

	#adds files (directoryname, contentType)
	file_add('thumbnail', 'thumbnails')

	#adds all the thubmbnail images to the database from the specified directory


	# directory = os.getcwd() + '/thumbnails'

	# for filename in os.listdir(directory):
	# 	if filename.endswith(".jpg"):

	# 		add_data(filename, 'thumbnail')

	# 	else:
	# 		print('Wrong file extention!')
	# print('hello')


	#   #adds all numpy files to the database from the specified directory
	#   directory = os.getcwd() + '/npy'

	#   for filename in os.listdir(directory):
	#       if filename.endswith(".npy"):

	#           add_data(filename, 'npy')

	#       else:
	#           print('Wrong file extention!')


	# get a specific file from the database and store it in the specified directory
	# im = get_specific('AgBH_10sec_SAXS.jpg', 'thumbnail')
	# im.save('get_specificImg.jpg')
	# print('File was successfully written to the specified directory!')


	#gets all files associated with an specific content type
	# and stores them to the CWD
	# get_all('npy')

	# finds the amount of files referencing a particular content type
	# amount = find_amount('thumbnail')
	# print(amount)


	#deletes all files for the given content type
	# del_all('npy')

	#deletes all files from the database
	# rm_all()

	# deletes specific file names from the database
	# del_specific('AgBH_10sec_SAXS.jpg', 'thumbnail')

	
#not part of the def main() function
if __name__ == '__main__':
	main()

