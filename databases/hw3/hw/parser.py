from __future__ import print_function  # Python 2/3 compatibility

import xml.sax
import sys, time

from pymongo import MongoClient


class XMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.pubkey = None
        self.tracking_elems = ["author", "title", "journal", "year", "booktitle"]
        self.tracking_table = { elem:None for elem in self.tracking_elems }
        self.authors = set()
        self.data = ""

        # mongodb related variables
        self.client = MongoClient("localhost", 27017)
        self.client.drop_database("dblp")

        self.dblp = self.client["dblp"]
        self.article = self.dblp["Article"]
        self.inproceedings = self.dblp["Inproceedings"]
        self.article_items = []
        self.inproceedings_items = []
        self.acount = 0
        self.icount = 0

    def startElement(self, tag, attributes):
        if tag == "article" or tag == "inproceedings":
            self.pubkey = attributes["key"]
            self.reset()
        elif tag in self.tracking_elems:
            self.data = ""

    def characters(self, content):
        self.data += content

    def endElement(self, tag):
        if tag == "article":
            self.add_article()
            self.reset()
        elif tag == "inproceedings":
            self.add_inproceedings()
            self.reset()
        elif tag == "author":
            self.authors.add(self.data)
        elif tag in self.tracking_elems:
            self.tracking_table[tag] = self.data
        elif tag == "dblp":
            self.__insert_article()
            self.__insert_inproceedings()

    def reset(self):
        self.tracking_table = { elem:None for elem in self.tracking_elems }
        self.authors = set()
        self.data = ""

    def add_article(self):
        item = {}
        if self.pubkey is not None:
            item["pubkey"] = self.pubkey
        if self.tracking_table["title"] is not None:
            item["title"] = self.tracking_table["title"]
        if self.tracking_table["booktitle"] is not None:
            item["journal"] = self.tracking_table["journal"]
        if self.tracking_table["year"] is not None:
            item["year"] = int(self.tracking_table["year"])
        if self.authors is not None and len(self.authors) > 0:
            item["authors"] = list(self.authors)
        self.article_items.append(item)
        if len(self.article_items) == 10000:
            self.__insert_article()

    def __insert_article(self):
        if len(self.article_items) > 0:
            self.acount += 1
            # print("Article:", self.acount)
            self.article.insert_many(self.article_items)
        self.article_items = []

    def add_inproceedings(self):
        item = {}
        if self.pubkey is not None:
            item["pubkey"] = self.pubkey
        if self.tracking_table["title"] is not None:
            item["title"] = self.tracking_table["title"]
        if self.tracking_table["booktitle"] is not None:
            item["booktitle"] = self.tracking_table["booktitle"]
        if self.tracking_table["year"] is not None:
            item["year"] = int(self.tracking_table["year"])
        if self.authors is not None and len(self.authors) > 0:
            item["authors"] = list(self.authors)
        self.inproceedings_items.append(item)
        if len(self.inproceedings_items) == 10000:
            self.__insert_inproceedings()

    def __insert_inproceedings(self):
        if len(self.inproceedings_items) > 0:
            self.icount += 1
            # print("Inproceedings:", self.icount)
            self.inproceedings.insert_many(self.inproceedings_items)
        self.inproceedings_items = []


def parse(input_xml):
    start_time = time.time()
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    parser.setContentHandler(XMLHandler())
    parser.parse(input_xml)
    print("Converted " + input_xml + " successfully.")
    print("Converting time = " + str(time.time() - start_time) + " (sec)\n")


if __name__ == "__main__":
    input_xml = sys.argv[1]

    parse(input_xml)
