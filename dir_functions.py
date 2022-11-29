import requests
import pandas as pd
import json
from datetime import date
import os
import random
from collections import OrderedDict
import re


# helper method to get data from firebase
def get_file(path):
    path = path.split(".")
    url = "https://namenode-1357e-default-rtdb.firebaseio.com/C/" + path[0] + ".json"
    r = requests.get(url)
    d = r.json()
    return d


# helper method to get key when you put in the value
def get_key(val, dictionary):
    for key, value in dictionary.items():
        if val == value:
            return key


# helper function to locate file in a nested json file
def find(data, k):
    if isinstance(data, list):
        for i in data:
            for item in find(i, k):
                yield item
    elif isinstance(data, dict):
        if k in data:
            yield data[k]
        for j in data.values():
            for x in find(j, k):
                yield x


# helper function to read request response
def read(r):
    if type(r.json()) is dict:
        r5 = dict(r.json())
        return pd.DataFrame(r5.values())
    else:
        d = [elem for elem in r.json() if elem != None]
        df = pd.DataFrame.from_records(d)
        return df


def mkdir(path):
    if "/" in path:
        directory = path.rsplit("/", 1)
        url = "https://namenode-1357e-default-rtdb.firebaseio.com/C/" + directory[0] + ".json"
        r = requests.patch(url, data=json.dumps({directory[1]: 0}))
        r
    else:
        url = "https://namenode-1357e-default-rtdb.firebaseio.com/C/" + ".json"
        r = requests.patch(url, data=json.dumps({path: 0}))
        r


def ls(path):
    try:
        url = "https://namenode-1357e-default-rtdb.firebaseio.com/C" + path + ".json"
        r = requests.get(url)
        response = r.json()
        return list(response.keys())
    except AttributeError:
        empty = []
        return empty


def rm(file_path):
    path = file_path.split(".")
    url = "https://namenode-1357e-default-rtdb.firebaseio.com/C" + path[0] + ".json"
    r = requests.get(url)
    response = r.json()
    try:
        for i in response["location"].values():
            r2 = requests.delete(i)
            r2
        r3 = requests.delete(url)
        r3
    except KeyError:
        print("Make sure path id correct. You can only delete a file not a folder")
    except TypeError:
        print("Make sure path id correct. You can only delete a file not a folder")


def cat(file_path):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option("display.expand_frame_repr", False)
    pd.set_option('display.max_colwidth', 40)
    file = get_file(file_path)
    frames = []
    for i in file["location"].values():
        data = requests.get(i)
        frame = read(data)
        frames.append(frame)
    if file["partition_type"] == "column":
        result = pd.concat(frames, axis=1)
    else:
        result = pd.concat(frames)
    print(result.reset_index(drop=True))


def put(file_name, path, partitions, sort):
    df = pd.read_csv(file_name)
    f = file_name.split(".")
    file_check = ls(path)
    if f[0] in file_check:
        print("file already exists. Please try renaming your file and uploading it again.")
    else:
        columns_correction = []
        metadata = {}
        location = {}
        columns = {}
        for i in df.columns:
            x = i.replace(" ", "_")
            x = x.replace(".", "")
            x = x.lower()
            columns_correction.append(x)
        df.columns = columns_correction
        file = file_name.split(".")
        if sort == "row":
            partition_index = round(len(df) / partitions)
            for i in range(1, partitions + 1):
                partition_name = file[0] + str(i)
                if i == 1:
                    p = df.iloc[:partition_index, :]
                elif i == partitions:
                    p = df.iloc[partition_index * (partitions - 1):, :]
                else:
                    p = df.iloc[partition_index * (i - 1):partition_index * i, :]
                nodes = ["https://datanode3-default-rtdb.firebaseio.com/",
                         "https://datanode-291fa-default-rtdb.firebaseio.com/",
                         "https://datanode2-default-rtdb.firebaseio.com/"]
                num1 = random.randint(0, 2)
                url = nodes[num1] + partition_name + ".json"
                df2 = p.to_json(orient="index")
                r = requests.post(url, data=df2)
                r
                r2 = requests.get(url)
                response = r2.json()
                keys = list(response.keys())
                url = url.replace(".json", "")
                url = url + "/" + keys[0] + ".json"
                location[partition_name] = url
        elif sort.lower() == "column":
            partition_index = round(len(df.columns) / partitions)
            for i in range(1, partitions + 1):
                partition_name = file[0] + str(i)
                if i == 1:
                    p = df.iloc[:, :partition_index]
                elif i == partitions:
                    p = df.iloc[:, partition_index * (partitions - 1):]
                else:
                    p = df.iloc[:, partition_index * (i - 1):partition_index * i]
                nodes = ["https://datanode3-default-rtdb.firebaseio.com/",
                         "https://datanode-291fa-default-rtdb.firebaseio.com/",
                         "https://datanode2-default-rtdb.firebaseio.com/"]
                num1 = random.randint(0, 2)
                url = nodes[num1] + partition_name + ".json"
                columns[partition_name] = list(p.columns)
                df2 = p.to_json(orient="index")
                r = requests.post(url, data=df2)
                r
                r2 = requests.get(url)
                response = r2.json()
                keys = list(response.keys())
                url = url.replace(".json", "")
                url = url + "/" + keys[
                    0] + ".json"
                location[partition_name] = url
        stat = os.stat(file_name)
        if sort == "column":
            metadata["columns"] = columns
        metadata["location"] = location
        metadata["partitions"] = partitions
        metadata["date_modified"] = str(date.today())
        metadata["size"] = stat.st_size
        metadata["partition_type"] = sort
        sample2 = "https://namenode-1357e-default-rtdb.firebaseio.com/C/" + path + ".json"
        r2 = requests.patch(sample2, data=json.dumps({file[0]: metadata}))
        r2


def getPartitionLocation(file, path):
    r = requests.get("https://namenode-1357e-default-rtdb.firebaseio.com/C" + path + ".json")
    d = r.json()
    f = file.split(".")
    hold = list(find(d, f[0]))
    try:
        for i in hold[0]["location"]:
            print(i + " : " + hold[0]["location"][i])
    except IndexError:
        print("File not located")
    except KeyError:
        print("File not found")


def readPartition(file, partition, path):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option("display.expand_frame_repr", False)
    pd.set_option('display.max_colwidth', 1000)

    f = file.split(".")
    partition_name = f[0] + str(partition)
    r = requests.get("https://namenode-1357e-default-rtdb.firebaseio.com/C" + path + ".json")
    response = r.json()
    hold = list(find(response, f[0]))
    url = hold[0]["location"][partition_name]
    r = requests.get(url)
    frame = read(r)
    print(frame)


# test = 'Select breed_name, intelligence, adaptability From dogs Where breed_name = "Akita"'
# test = input("input select statemnt ")


def map(str, file_location):
    str = re.sub(r"\s+", "", str.lower())
    try:
        file = re.search(r'from(.*?)where', str).group(1)
    except AttributeError:
        file = re.search(r'from(.*)', str).group(1)
    col_display = re.search(r'select(.*?)from', str).group(1)
    try:
        condition = re.search(r'where(.*)', str).group(1)
    except AttributeError:
        condition = 0
    print(condition)
    col = col_display.split(",")
    print(col)
    if condition != 0:
        print("happened")
        if "=" in condition:
            condition = condition.replace("=", "==")
    print(condition)
    # print(file, col, condition)
    url = "https://namenode-1357e-default-rtdb.firebaseio.com/C" + file_location + ".json"
    r = requests.get(url)
    data = r.json()
    loc = data["location"]
    partition_type = data["partition_type"]
    results = []
    if partition_type == "row":
        for i in loc.values():
            r = requests.get(i)
            par = r.json()
            frame = read(r)
            frames = frame.convert_dtypes(infer_objects=False)
            if condition != 0:
                df = frames.query(condition)
                if df.empty == False:
                    results.append(df[col])

            elif col[0] == "*":
                results.append(frames)
            elif col[0] != "*":
                results.append(frames[col])
            return results
            # for j in results:
            #     print(j)
    # elif partition_type == "column":
    #     if

# map(test)


def app():
    manual = {"ls": "ls is used to display all the files or folders in a folder. If ls is called with no path, " \
                    "it will print the file/folders in the current directory ",
              "mkdir": 'mkdir is used to create a folder. it is called like:\n \nmkdir /john/documents\n \nIf mkdir is called with no path,it will create a folders in the current directory.',
              "cat": 'cat is used to display the contents of a file: it is called like: \n\ncat /john/documents/hello.csv\n\n if the file to be displayed is in the current folder the file name can be entered ',
              "rm": 'rm is used to to remove a file from a folder. it can be called like:\n\nrm /john/documents/hello.csv\n\n if the file is in the current folder then it can be deleted by just specifying the name of the file ',
              "put": 'put is used to add a file to a folder. The name of the file, path, number of partitions and partition method must be specified as so:\n\nput dogs.csv /john/documents 4 row\n\nthe file can either be partitioned by row or column. this should be specified in the function call\nif the file is to be put in the current directory the function can be called without a path ',
              "getPartitionLocations": 'getPartitionLocations is used to get the locations of the different partitions of the file. it can be called like:\n\ngetPartitionLocations hello.csv /john/documents\n\n the function is called with the name of the file and the location. If the function is called with just the name of the file it would search in the current directory',
              "readPartition": 'readPartition is used to display the contents of a specific partition. it is called like:\n\nreadPartition hello.csv 2\n\nThis will display the contents of the second partition of the file.'}

    print(
        "------- Welcome to the EFDS system. The system supports the following commands: ls, mkdir, cat, rm, put, "
        "getPartitionLocaions amd readPartitions\n "
        "------- To view the manual for any function please type man followed by the name of the function.\n"
        "------- To view the entire manual type man")
    response = ""
    main = "EDFS:C"
    current_directory = ""

    while response != "exit":
        display = main + current_directory + ">"
        response = input(display).lower()
        if response.startswith("ls"):
            r = response.split(" ")
            try:
                if len(r) == 1:
                    files = ls(current_directory)
                    for i in range(0, len(files)):
                        if i == len(files) - 1:
                            print(files[i])
                        else:
                            print(files[i], end=" ")
                elif len(r) == 2:
                    files = ls(current_directory + r[1])
                    for i in range(0, len(files)):
                        if i == len(files) - 1:
                            print(files[i])
                        else:
                            print(files[i], end=" ")
                else:
                    print("Please enter only the command and the location of the file")
            except AttributeError:
                print("No files in current directory")
        if response.startswith("mkdir"):
            r = response.split(" ")
            if "/" in r[1]:
                mkdir(r[1])
            else:
                path = r[1]
                mkdir(current_directory + "/" + path)
        if response.startswith("cd"):
            try:
                r = response.split(" ")
                if "/" in r[1]:
                    current_directory = r[1]
                else:
                    current_directory += "/" + r[1]
            except IndexError:
                print("directory does not exist")
                continue
        if response.startswith("put"):
            r = response.split()
            try:
                file_details = r[1:]
                if len(file_details) == 3:
                    put(file_details[0], current_directory, int(file_details[1]), file_details[2])
                else:
                    put(file_details[0], file_details[1], int(file_details[2]), file_details[3])
            except FileNotFoundError:
                print("File does not exist. Please make sure file is in folder where application is being run")
        if response.startswith("man"):
            r = response.split(" ")
            if len(r) < 2:
                for i in manual.items():
                    print(i[0], ":", i[1])
                    print("------------------------------------------------------------------------")
            else:
                print(manual[r[1]])
        if response.startswith("rm"):
            r = response.split(" ")
            if "/" in r[1]:
                print(current_directory + r[1])
                rm(r[1])
            else:
                print(current_directory + "/" + r[1])
                rm(current_directory + "/" + r[1])
        if response.startswith("cat"):
            r = response.split(" ")
            if "/" in r[1]:
                print(cat(r[1]))
            else:
                print(current_directory + "/" + r[1])
                print(cat(current_directory + "/" + r[1]))
        if response.startswith("getpartitionlocation"):
            r = response.split(" ")
            if len(r) == 2:
                getPartitionLocation(r[1], current_directory)
            else:
                getPartitionLocation(r[1], r[2])
        if response.startswith("readpartition"):
            try:
                r = response.split(" ")
                if len(r) == 3:
                    readPartition(r[1], r[2], current_directory)
                else:
                    readPartition(r[1], r[2], r[3])
            except KeyError:
                print("Check partition number or verify the file name")
        if response.startswith("select"):
            map(response, current_directory)

app()

# mkdir("/paper/jules/arnold/terry")
# ls("/paper/jules")
# put("forestfires.csv","/julia/jules",3, "row")
# getPartitionLocation("forestfires.csv", "/john")
# readPartition("dogs.csv", 2)
# rm("/julia/jules/forestfires.csv")
# cat("/julia/jullete/forestfires.csv")
