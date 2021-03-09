import requests
import os
import sys
import json

def checklist(data):
    if len(data) > 0:
        return True


def del_slash(path):
    if path[0] == "/":
        path = path[1:]
    return path


def mkdir(data):
    path = "".join(data)
    r = requests.put(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{path}?user.name={username}&op=MKDIRS")
    if r.status_code == 200:
        print(f"Path \"{path}\" created.")
    else:
        print("Error:", r.status_code)


def put(data):

    file = "".join(data)
    r = requests.put(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{file}?op=CREATE&user.name={username}&overwrite=true&noredirect=true&replication=1")
    link = json.loads(r.text)["Location"]
    print(link)
    r = requests.put(link, data=open(localPath+file))
    if r.status_code == 201:
        print(f"File \"{file}\" uploaded")
    else:
        print("Error:", r.status_code)

def get(data):
    file = "".join(data)
    r = requests.get(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{file}?op=OPEN&user.name={username}")
    if r.status_code == 200:
        f = open(localPath + file, "w")
        f.write(r.text)
        f.close()
        print(f"File \"{file}\" downloaded")
    else:
        print("Error:", r.status_code)


def append(data):
    localfile = "".join(data[0])
    hdfsfile = "".join(data[1])
    r = requests.post(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{hdfsfile}?op=SETREPLICATION&replication=1")
    r = requests.post(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{hdfsfile}?op=APPEND&user.name={username}&noredirect=true")
    link = json.loads(r.text)["Location"]
    print(link)
    r2 = requests.post(link, data=open(localPath + localfile))
    if r2.status_code == 200:
        print(f"File \"{localfile}\" was append to \"{hdfsfile}\"")
    else:
        print("Error:", r2.status_code)


def delete(data):
    path = "".join(data)
    r = requests.delete(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}"
                        f"{path}?user.name={username}&op=DELETE&recursive=true")
    if r.status_code == 200:
        print(f"Path {path} deleted.")
    else:
        print(f"Error: {r.status_code}")


def ls():
    r = requests.get(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}?op=LISTSTATUS&user.name=kali")
    ls = r.json()["FileStatuses"]["FileStatus"]
    for el in ls:
        print(el.get("pathSuffix"), end="   ")
    print()


def cd(data):
    global hdfsPath
    path = "".join(data)
    if path == "..":
        hdfsPath = "/".join(hdfsPath.split("/")[:-2]) + "/"
        if hdfsPath == "//":
            hdfsPath = "/"
    else:
        r = requests.get(f"http://{hostname}:{port}/webhdfs/v1{hdfsPath}{path}?op=GETFILESTATUS")
        if r.status_code == 200:
            hdfsPath = hdfsPath + del_slash(path) + "/"
        elif r.status_code == 404:
            print("No such file or directory")


def lls():
    files = os.listdir(localPath)
    for i in files:
        print(i, end="   ")
    print()


def lcd(data):
    global localPath
    path = "".join(data)
    if path == "..":
        localPath = "/".join(localPath.split("/")[:-2]) + "/"
        print(localPath)
        if localPath == "//":
            localPath = "/"
    else:
        if os.path.exists(localPath + path):
            localPath = localPath + del_slash(path) + "/"
        else:
            print("No such file or directory")


def HDFS_start():
    print("HDFS client started. Write command:\n")
    while True:
        print()
        print("Local path: ", localPath, sep="")
        print("HDFS path: ", hdfsPath, sep="")
        print("~$ ", end="")
        cmd = input().split()
        if cmd[0] == 'mkdir':
            cmd.remove('mkdir')
            if checklist(cmd):
                mkdir(cmd)
            else:
                print("Error: wrong path")
        elif cmd[0] == 'put':
            cmd.remove('put')
            if checklist(cmd):
                put(cmd)
            else:
                print("Error: wrong path")
        elif cmd[0] == 'get':
            cmd.remove('get')
            if checklist(cmd):
                get(cmd)
            else:
                print("Error: wrong path")
        elif cmd[0] == 'append':
            cmd.remove('append')
            if checklist(cmd):
                if len(cmd) == 2:
                    append(cmd)
                else:
                    print("Error: wrong path")
            else:
                print("Error: wrong path")
        elif cmd[0] == 'delete':
            cmd.remove('delete')
            if checklist(cmd):
                delete(cmd)
            else:
                print("Error: wrong path")
        elif cmd[0] ==  'ls':
            cmd.remove('ls')
            ls()
        elif cmd[0] == 'cd':
            cmd.remove('cd')
            if checklist(cmd):
                cd(cmd)
            else:
                print("Error: wrong path")
        elif cmd[0] == 'lls':
            cmd.remove('lls')
            lls()
        elif cmd[0] == 'lcd':
            cmd.remove('lcd')
            if checklist(cmd):
                lcd(cmd)
            else:
                print("Error: wrong path")
        elif 'exit' in cmd:
            break
        else:
            print("Wrong command. You can use commands from list:\n"
                  "mkdir <hdfs path>\n"
                  "put <local file> <hdfs path>\n"
                  "get <hdfs file> <local path>\n"
                  "append <local file> <hdfs file>\n"
                  "delete <hdfs path>\n"
                  "ls\n"
                  "cd\n"
                  "lls\n"
                  "lcd\n"
                  "exit\n")
                  
if __name__ == "__main__":
    global hostname
    hostname = sys.argv[1] # localhost
    global port
    port = sys.argv[2] #9870
    global username
    username = sys.argv[3] if sys.argv[3].isdigit else print("invalid")

    global hdfsPath
    hdfsPath = "/"
    global localPath
    localPath = "/Users/stanislavdanilov/Desktop/test2/"
    HDFS_start()
