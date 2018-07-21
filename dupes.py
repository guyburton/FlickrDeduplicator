
import flickr_api
import sys
from collections import defaultdict
import hashlib
import os
import tempfile
import urllib
import sqlite3

import configparser

debug = False

def initDb():
    global conn

    conn = sqlite3.connect('dedupe.db')
    c = conn.cursor()
    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS image_hashes
                 (id text, hash text)''')

    cache_size = c.execute('''SELECT COUNT(id) FROM image_hashes''').fetchone()

    if cache_size:
        print ('Found ' + str(cache_size[0]) + ' hashes in cache')

    conn.commit()

def initFlickrApi():
    config = configparser.ConfigParser()
    config.read('config.ini')

    print("Authenticating with Flickr")
    if 'FLICKR' not in config or 'API_KEY' not in config['FLICKR']:
        raise Exception('You need to set your API key in config.ini (see config.ini.example)')
    
    flickr_api.set_keys(api_key = config['FLICKR']['API_KEY'], api_secret = config['FLICKR']['API_SECRET'])
    flickr_api.set_auth_handler('auth.txt')

class DPhoto():
    def __init__(self, photo, count):
        self.photo = photo
        self.count = count

    def getTitleNoExtension(self):
        return self.photo.title[:self.photo.title.rfind('.')] + '-' + str(self.count)

    def getExtension(self):
        return self.photo.title[self.photo.title.rfind('.'):]

    def getTitle(self):
        return self.getTitleNoExtension() + self.getExtension()

    def getOriginalTitle(self):
        return self.photo.title

    def getId(self):
        return self.photo.id

    def hash(self):
        photo_file = self.photo.getPhotoFile('Square')
        
        if debug:
            print(photo_file)

        with urllib.request.urlopen(photo_file) as r:
            buf = r.read()
            hasher = hashlib.md5()
            hasher.update(buf)
            imagehash = hasher.hexdigest()
            if debug:
                print ('Photo id: ' + self.photo.id + ' ' + self.photo.title + ' hash: ' + imagehash)
            return imagehash

    def delete(self):
        # self.photo.delete
        pass

def store_hash(dphoto):
    c = conn.cursor()

    c.execute("SELECT hash from image_hashes where id=:id", {"id" : str(dphoto.getId())})
    imagehash = c.fetchone()

    if imagehash:
        if debug:
            print("Found cached image hash for " + dphoto.getId())
        return imagehash

    imagehash = dphoto.hash()
    c.execute("INSERT INTO image_hashes VALUES (?,?)", (str(dphoto.getId()), str(hash)))
    conn.commit()
    if debug:
        print("Cached hash for " + dphoto.getId())
    return imagehash

def getAllPhotoInfo():
    user = flickr_api.test.login()

    photos = user.getPhotos(per_page=500)
    total = photos.info.total
    print('Found ' + str(total) + ' photos')

    indexedById = dict()

    pages = photos.info.pages
    for page in range(1, pages + 1):
        print('Indexing photos, page ' + str(page) + ' / ' + str(pages))
        photos = user.getPhotos(page=page, per_page=500)
        for photo in photos:
            if photo.id in indexedById:
                raise Exception('Already seen id ' + photo.id + ' (must be a programming error)')
            indexedById[photo.id] = photo

    return indexedById.values()

def hashPhotos(photos):
    hashes = defaultdict(set)   

    count = 0       
    for photo in photos:
        dphoto = DPhoto(photo, count)
        
        hashes[store_hash(dphoto)].add(dphoto)
        count += 1

    print ("Hashed " + str(len(hashes)) + " images")
    return hashes


def findDuplicates():
    allPhotos = getAllPhotoInfo()

    hashes = hashPhotos(allPhotos)

    to_delete = set()   
    for photos in hashes.values():
        if len(photos) <= 1:
            continue

        example_photo = photos.pop()
        print("Found " + str(len(photos)) + " duplicates of " + example_photo.getOriginalTitle())

        for photo in photos:
            print("Will delete " + photo.getId())
            to_delete.add(photo)
        
    for photo in to_delete:
        print ("Deleting " + photo.getOriginalTitle() + ' ' + str(photo.getId()) ) 
        photo.delete()


if __name__ == "__main__":
    initDb()
    initFlickrApi()
    findDuplicates()

