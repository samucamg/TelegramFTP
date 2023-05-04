from os import environ
from os.path import exists
from pymongo import MongoClient
from pathlib import PurePosixPath
from re import compile
if exists(".env"):
    from dotenv import load_dotenv
    load_dotenv()

login_regex = compile(r'^[a-zA-Z0-9_]{1,64}$')

MONGODB = environ.get("MONGODB") or input("MongoDB connect string: ")
db = MongoClient(MONGODB).ftp["users"]

class Permission:
    def __init__(self, path, readable=False, writable=False):
        self.path = path
        self.read = readable or writable
        self.write = writable

class User:
    def __init__(self, login, password, permissions):
        self.login = login
        self.password = password
        self.permissions = permissions

    def formatPermissions(self):
        for perm in self.permissions:
            print(f"Path: {repr(perm.path)}, Read: {perm.read}, Write: {perm.write}")

    def addPermission(self, perm):
        self.permissions.append(perm)

    def removePermission(self, perm):
        self.permissions.remove(perm)

def getInput(arr, objs=None):
    o = bool(objs) and len(arr) == len(objs)
    while True:
        for i, line in enumerate(arr):
            print(f"{i+1}. {line}")
        try:
            _sel = input("Select: ")
            sel = int(_sel)
            if sel > len(arr) or sel <= 0:
                raise ValueError
            return sel-1 if not o else objs[sel-1]
        except ValueError:
            print(f"Invalid input: {_sel}")

def changeUserPassword(user):
    newPassw = input("Nova Senha: ")
    if newPassw == user.password:
        print("A Nova senha não pode ser igual a anterior")
        return
    db.update_one({"login": user.login}, {"$set": {"password": newPassw}})
    user.password = newPassw
    print("Senha alterada com sucesso.")

def editPermissions(user):
    while True:
        print("Action:")
        action = getInput(["Add permission", "Edit permission", "Delete permission", "Back"])
        if action == 0:
            path = input("Path: ")
            path = PurePosixPath(path)
            if not path or not path.is_absolute() or [p for p in user.permissions if PurePosixPath(p.path) == path]:
                print("Invalid path")
                continue
            read = input("Read permission (yes/no): ").lower().strip() in ["yes", "y", "true", "1"]
            write = input("Write permission (yes/no): ").lower().strip() in ["yes", "y", "true", "1"]
            db.update_one({"login": user.login}, {"$push": {"permissions": {"path": str(path), "readable": read, "writable": write}}})
            perm = Permission(str(path), read, write)
            user.addPermission(perm)
            p = f"Path: {repr(perm.path)}, Read: {perm.read}, Write: {perm.write}"
            print(f"Permission \"{p}\" for user {user.login} created.")
            continue
        elif action in [1, 2]:
            print("Permissions:")
            perms = user.permissions.copy()
            perms = [f"Path: {repr(p.path)}, Read: {p.read}, Write: {p.write}" for p in perms]
            perms.append("Back")
            perm = getInput(perms, user.permissions.copy()+[None])
            if perm is None:
                continue
            if action == 1:
                ch = getInput(["Read", "Write", "Back"])
                read = perm.read
                write = perm.write
                if ch == 0:
                    read = input("Read permission (yes/no): ").lower().strip() in ["yes", "y", "true", "1"]
                elif ch == 1:
                    write = input("Write permission (yes/no): ").lower().strip() in ["yes", "y", "true", "1"]
                elif ch == 2:
                    continue
                idx = user.permissions.index(perm)
                db.update_one({"login": user.login}, {"$set": {f"permissions.{idx}.readable": read, f"permissions.{idx}.writable": write}})
                perm.read = read
                perm.write = write
                continue
            elif action == 2:
                if input("Write 'delete' to delete permission: ") != "delete":
                    print("Invalid input")
                    continue
                db.update_one({"login": user.login}, {"$pull": {"permissions": {"path": perm.path}}})
                user.removePermission(perm)
                print("Permission deleted")
        elif action == 3:
            return

def printUserData(user):
    while True:
        print(f"Login: {user.login}")
        print(f"Password: {'*'*len(user.password)}")
        print("Actions:")
        action = getInput(["Show password", "Set password", "Show permissions", "Edit permissions", "Delete user", "Back"])
        if action == 0:
            print(f"Password: {user.password}\nPress enter to continue...")
            input()
            continue
        if action == 1:
            changeUserPassword(user)
            continue
        elif action == 2:
            print("Permissions:")
            user.formatPermissions()
            print(f"Press enter to continue...")
            input()
            continue
        elif action == 3:
            editPermissions(user)
            continue
        elif action == 4:
            login = input(f"Enter '{user.login}' or 'delete user' to delete this user: ")
            if login != user.login and login != "delete user":
                print("Invalid input.")
                continue
            db.delete_one({"login": user.login})
            return
        elif action == 5:
            return

def showUsers():
    while True:
        print("Loading...", end="")
        _users = db.find({})
        print("\rUsers:     ")
        users = []
        for _user in _users:
            perms = [Permission(**perm) for perm in _user.get("permissions", [])]
            users.append(User(_user["login"], _user["password"], perms))
        t_users = [user.login for user in users]+["Back"]
        u = getInput(t_users, users.copy()+[None])
        if u is None:
            return
        printUserData(u)

def addUser():
    login = input("Login: ")
    if not login_regex.match(login):
        print("Login can include only this characters: \"a-Z, 0-9, _\" and have lenght <= 64")
        return
    u = list(db.find({"login": login}))
    if u:
        print(u)
        print("User with this login already exists")
        return
    password = input("Password: ")
    db.insert_one({"login": login, "password": password, "permissions": []})
    print(f"User \"{login}\" created")

def main():
    while True:
        try:
            action = getInput(["Show users", "Add user", "Exit"])
        except KeyboardInterrupt:
            print()
            return
        try:
            if action == 0:
                showUsers()
                continue
            elif action == 1:
                addUser()
                continue
            elif action == 2:
                return
        except KeyboardInterrupt:
            print()
            continue

if __name__ == "__main__":
    main()
