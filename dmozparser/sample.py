
#!/usr/bin/env python

from parser import DmozParser
from handlers import JSONWriter

class LawrenceFilter:
  def __init__(self):
    self._file = open("seeds.txt", 'w')

  def page(self, page, content):
      if page != None and page != "":
          topic = content['topic']
          if topic.find('Venture') > 0 or topic.find('Financial_Services')  >  0 :
              self._file.write(page + " " + topic + "\n")
              print("found page %s in topic %s" % (page , topic))

  def finish(self):
    self._file.close()


parser = DmozParser()
parser.add_handler(
    LawrenceFilter()
    #JSONWriter('output.json')
)
parser.run()
