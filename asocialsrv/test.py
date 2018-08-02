import os, json

def listRes(pyp, build, shortcuts, cp = None):
    rootdir = pyp + "/build/" + build
    lx = len(rootdir)
    cx = "/" + cp + "/$ui" if (cp is not None and len(cp) != 0) else "/$ui"
    lst = []
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            #print os.path.join(subdir, file)
            filepath = subdir + os.sep + file
            lst.append(cx + filepath[lx:])
    d1 = "const shortcuts = " + json.dumps(shortcuts) + ";\n"
    d2 = "const build = \"" + build + "\";\nconst cp = \"" + ("" if cp is None else cp) + "\";\nconst lres = [\n"
    f = open("ui/" + build + "/src/sw.js", "r")
    t = f.read();
    return d1 + d2 + ",\n".join(lst) + "\n];\n" + t

shortcuts = {"a":"prod-index", "d":"demo-home"}
pyp = os.path.dirname(__file__)
build = "1.1"

print(listRes(pyp, build, shortcuts, "cp"))