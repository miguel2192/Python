"""
    Description: Converts a given XML file(s) to Json(s) and writes than to the CWD
    :input param file(s) directory: Directory
    :input param json name: Output json name + extention

    Miguel Rodriguez Extended functionality

"""

import json
import os

from lxml import etree

def xml_to_json(xml_input, json_output):
    '''Converts an xml file to json.'''
    dict_to_json(etree_to_dict(xml_to_etree(xml_input), True), json_output)

def xml_to_etree(xml_input):
    '''Converts xml to a lxml etree.'''
    f = open(xml_input, 'r')
    xml = f.read()
    f.close()
    return etree.HTML(xml)

def etree_to_dict(tree, only_child):
    '''Converts an lxml etree into a dictionary.'''
    mydict = dict([(item[0], item[1]) for item in tree.items()])
    children = tree.getchildren()
    if children:
        if len(children) > 1:
            mydict['children'] = [etree_to_dict(child, False) for child in children]
        else:
            child = children[0]
            mydict[child.tag] = etree_to_dict(child, True)
    if only_child:
        return mydict
    else:
        return {tree.tag: mydict}

def dict_to_json(dictionary, json_output):
    '''Coverts a dictionary into a json file.'''
    directory =  os.getcwd() + '/resultsJson/'
    fullpath = os.path.join(directory, json_output)
    f = open(fullpath, 'w')
    f.write(json.dumps(dictionary, sort_keys=True, indent=4))
    f.close()

def main():
    directory =  os.getcwd() + '/newmultiview/exp_visualization/SciAnalysis/results/'

    count = 0

    for filename in os.listdir(directory):
      if filename.endswith(".xml"):
        print('Converting XML file to Json...')

        jsonName = filename + '.json'
        xml_to_json(directory + filename, jsonName)
      else:
        print('File extention is not XML!')
    print("""'All XML files in the given directory were successfully converted into their Json's counterparts!""")


if __name__ == "__main__":
        main()