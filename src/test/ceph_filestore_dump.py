#! /usr/bin/python

from subprocess import call
from subprocess import check_output
import os
import time
import sys
import re
import string


def wait_for_health():
    print "Wait for health_ok...",
    while call("./ceph health 2> /dev/null | grep -v HEALTH_OK > /dev/null", shell=True) == 0:
        time.sleep(5)
    print "DONE"


def get_pool_id(name, nullfd):
    cmd = "./ceph osd pool stats {pool}".format(pool=name).split()
    # pool {pool} id # .... grab the 4 field
    return check_output(cmd, stderr=nullfd).split()[3]


# return a sorted list of unique PGs given a directory
def get_pgs(DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0 ]
    PGS = []
    endhead = re.compile("{id}.*_head$".format(id=ID))
    for d in OSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        PGS += [f for f in os.listdir(SUBDIR) if os.path.isdir(os.path.join(SUBDIR, f)) and endhead.match(f)]
    PGS = [re.sub("_head", "", p) for p in PGS]
    return sorted(set(PGS))


# return a sorted list of PGS a subset of ALLPGS that contain objects with prefix specified
def get_objs(ALLPGS, prefix, DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0 ]
    PGS = []
    for d in OSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        for p in ALLPGS:
            PGDIR = p + "_head"
            if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
                continue
            FINALDIR = os.path.join(SUBDIR, PGDIR)
            # See if there are any objects there
            if [f for f in os.listdir(FINALDIR) if os.path.isfile(os.path.join(FINALDIR, f)) and string.find(f, prefix) == 0 ]:
                PGS += [p]
    return sorted(set(PGS))


# return a sorted list of OSDS which have data from a given PG
def get_osds(PG, DIR):
    ALLOSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0 ]
    OSDS = []
    for d in ALLOSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        PGDIR = PG + "_head"
        if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
            continue
        OSDS += [d]
    return sorted(OSDS)


def get_lines(filename):
    tmpfd = open(filename, "r")
    line = True
    lines = []
    while line:
        line = tmpfd.readline().rstrip('\n')
        if line:
            lines += [line]
    tmpfd.close()
    os.unlink(filename)
    return lines


def vstart(new):
    print "vstarting....",
    OPT = new and "-n" or ""
    call("OSD=4 ./vstart.sh -l {opt} -d > /dev/null 2>&1".format(opt=OPT), shell=True)
    print "DONE"


def test_failure(cmd, errmsg):
    ttyfd = open("/dev/tty", "rw")
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    tmpfd = open(TMPFILE, "w")

    ret = call(cmd, shell=True, stdin=ttyfd, stdout=ttyfd, stderr=tmpfd)
    ttyfd.close()
    tmpfd.close()
    if ret == 0:
        print "Should have failed, but got exit 0"
        return 1
    lines = get_lines(TMPFILE)
    line = lines[0]
    if line == errmsg:
        print "Correctly failed with message \"" + line + "\""
        return 0
    else:
        print "Bad message to stderr \"" + line + "\""
        return 1


def main():
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    nullfd = open(os.devnull, "w")

    OSDDIR = "dev"
    REP_POOL = "rep_pool"
    REP_NAME = "REPobject"
    EC_POOL = "ec_pool"
    EC_NAME = "ECobject"
    NUM_OBJECTS = 40
    ERRORS = 0
    pid = os.getpid()
    TESTDIR = "/tmp/test.{pid}".format(pid=pid)
    DATADIR = "/tmp/data.{pid}".format(pid=pid)

    vstart(new=True)
    wait_for_health()

    cmd = "./ceph osd pool create {pool} 12 12 replicated".format(pool=REP_POOL)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    REPID = get_pool_id(REP_POOL, nullfd)

    print "Created Replicated pool #{repid}".format(repid=REPID)

    cmd = "./ceph osd erasure-code-profile set testprofile ruleset-failure-domain=osd"
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd erasure-code-profile get testprofile"
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd pool create {pool} 12 12 erasure testprofile".format(pool=EC_POOL)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    ECID = get_pool_id(EC_POOL, nullfd)

    print "Created Erasure coded pool #{ecid}".format(ecid=ECID)

    print "Creating {objs} objects in replicated pool".format(objs=NUM_OBJECTS)
    cmd = "mkdir -p {datadir}".format(datadir=DATADIR)
    call(cmd, shell=True)

    db = {}

    objects = xrange(1, NUM_OBJECTS + 1)
    for i in objects:
        NAME = REP_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        cmd = "rm -f " + DDNAME
        call(cmd, shell=True)

        dataline = xrange(10000)
        f = open(DDNAME, "w")
        data = "This is the replicated data for " + NAME + "\n"
        for j in dataline:
            f.write(data)
        f.close()

        cmd = "./rados -p {pool} put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME)
        call(cmd, shell=True, stderr=nullfd)

        db[NAME] = {}

        keys = xrange(i)
        db[NAME]["xattr"] = {}
        for k in keys:
            if k == 0:
                continue
            mykey = "key{i}-{k}".format(i=i, k=k)
            myval = "val{i}-{k}".format(i=i, k=k)
            cmd = "./rados -p {pool} setxattr {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval)
            # print cmd
            call(cmd, shell=True)
            db[NAME]["xattr"][mykey] = myval

        # Create omap header in all objects but REPobject1
        if i != 1:
            myhdr = "hdr{i}".format(i=i)
            cmd = "./rados -p {pool} setomapheader {name} {hdr}".format(pool=REP_POOL, name=NAME, hdr=myhdr)
            # print cmd
            call(cmd, shell=True)
            db[NAME]["omapheader"] = myhdr

        db[NAME]["omap"] = {}
        for k in keys:
            if k == 0:
                continue
            mykey = "okey{i}-{k}".format(i=i, k=k)
            myval = "oval{i}-{k}".format(i=i, k=k)
            cmd = "./rados -p {pool} setomapval {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval)
            # print cmd
            call(cmd, shell=True)
            db[NAME]["omap"][mykey] = myval

    print "Creating {objs} objects in erasure coded pool".format(objs=NUM_OBJECTS)

    for i in objects:
        NAME = EC_NAME + "{num}".format(num=i)
        DDNAME = os.path.join(DATADIR, NAME)

        cmd = "rm -f " + DDNAME
        call(cmd, shell=True)

        f = open(DDNAME, "w")
        data = "This is the erasure coded data for " + NAME + "\n"
        for j in dataline:
            f.write(data)
        f.close()

        cmd = "./rados -p {pool} put {name} {ddname}".format(pool=EC_POOL, name=NAME, ddname=DDNAME)
        call(cmd, shell=True, stderr=nullfd)

        db[NAME] = {}

        db[NAME]["xattr"] = {}
        keys = xrange(i)
        for k in keys:
            if k == 0:
                continue
            mykey = "key{i}-{k}".format(i=i, k=k)
            myval = "val{i}-{k}".format(i=i, k=k)
            cmd = "./rados -p {pool} setxattr {name} {key} {val}".format(pool=EC_POOL, name=NAME, key=mykey, val=myval)
            # print cmd
            call(cmd, shell=True)
            db[NAME]["xattr"][mykey] = myval

        # Omap isn't supported in EC pools
        db[NAME]["omap"] = {}

    # print db

    call("./stop.sh", stderr=nullfd)

    ALLREPPGS = get_pgs(OSDDIR, REPID)
    # print ALLREPPGS
    ALLECPGS = get_pgs(OSDDIR, ECID)
    # print ALLECPGS

    OBJREPPGS = get_objs(ALLREPPGS, REP_NAME, OSDDIR, REPID)
    # print OBJREPPGS
    OBJECPGS = get_objs(ALLECPGS, EC_NAME, OSDDIR, ECID)
    # print OBJECPGS

    ONEPG = ALLREPPGS[0]
    # print ONEPG
    osds = get_osds(ONEPG, OSDDIR)
    ONEOSD = osds[0]
    # print ONEOSD

    # On export can't use stdout to a terminal
    cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type export --pgid {pg}".format(
        osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdout is a tty and no --file option specified")

    OTHERFILE = "/tmp/foo.{pid}".format(pid=pid)
    foofd = open(OTHERFILE, "w")
    foofd.close()

    # On import can't specify a PG
    cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --pgid {pg} --file {FOO}".format(osd=ONEOSD, pg=ONEPG, FOO=OTHERFILE)
    ERRORS += test_failure(cmd, "--pgid option invalid with import")

    os.unlink(OTHERFILE)
    cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --file {FOO}".format(
        osd=ONEOSD, FOO=OTHERFILE)
    ERRORS += test_failure(cmd, "open: No such file or directory")

    # On import can't use stdin from a terminal
    cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --pgid {pg}".format(
        osd=ONEOSD, pg=ONEPG)
    ERRORS += test_failure(cmd, "stdin is a tty and no --file option specified")

    # Test --type list and generate json for all objects
    print "Testing --type list by generating json for all objects"
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=pid)
    ALLPGS = OBJREPPGS + OBJECPGS
    for pg in ALLPGS:
        OSDS = get_osds(pg, OSDDIR)
        for osd in OSDS:
            cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type list --pgid {pg}".format(osd=osd, pg=pg)
            tmpfd = open(TMPFILE, "a")
            ret = call(cmd, shell=True, stdout=tmpfd)
            if ret != 0:
                print "Bad exit status {ret} from --type list request".format(ret=ret)
                ERRORS += 1

    tmpfd.close()
    lines = get_lines(TMPFILE)
    JSONOBJ = sorted(set(lines))

    # Test get-bytes
    print "Testing get-bytes and set-bytes"
    for basename in db.keys():
        file = os.path.join(DATADIR, basename)
        JSON = [l for l in JSONOBJ if l.find("\"" + basename + "\"") != -1]
        JSON = JSON[0]
        GETNAME = "/tmp/getbytes.{pid}".format(pid=pid)
        TESTNAME = "/tmp/testbytes.{pid}".format(pid=pid)
        SETNAME = "/tmp/setbytes.{pid}".format(pid=pid)
        for pg in OBJREPPGS:
            OSDS = get_osds(pg, OSDDIR)
            for osd in OSDS:
                DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                fname = [ f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f)) and string.find(f, basename + "_") == 0 ]
                if not fname:
                    continue
                fname = fname[0]
                try:
                    os.unlink(GETNAME)
                except:
                    pass
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' get-bytes {fname}".format(osd=osd, pg=pg, json=JSON, fname=GETNAME)
                ret = call(cmd, shell=True)
                if ret != 0:
                    print cmd
                    print "Bad exit status {ret}".format(ret=ret)
                    ERRORS += 1
                    continue
                cmd = "diff -q {file} {getfile}".format(file=file, getfile=GETNAME)
                ret = call(cmd, shell=True)
                if ret != 0:
                    print "Data from get-bytes differ"
                    print "Got:"
                    cat_file(GETNAME)
                    print "Expected:"
                    cat_file(file)
                    ERRORS += 1
                fd = open(SETNAME, "w")
                data = "put-bytes going into {file}\n".format(file=file)
                fd.write(data)
                fd.close()
                fd = open(SETNAME, "r")
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' set-bytes -".format(osd=osd, pg=pg, json=JSON)
                ret = call(cmd, shell=True, stdin=fd)
                fd.close()
                if ret != 0:
                    print "Bad exit status {ret} from set-bytes".format(ret=ret)
                    ERRORS += 1
                fd = open(TESTNAME, "w")
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' get-bytes -".format(osd=osd, pg=pg, json=JSON)
                ret = call(cmd, shell=True, stdout=fd)
                fd.close()
                if ret != 0:
                    print cmd
                    print "Bad exit status {ret} from get-bytes".format(ret=ret)
                    ERRORS += 1
                cmd = "diff -q {setfile} {testfile}".format(setfile=SETNAME, testfile=TESTNAME)
                ret = call(cmd, shell=True)
                if ret != 0:
                    print "Data after set-bytes differ"
                    print "Got:"
                    cat_file(TESTNAME)
                    print "Expected:"
                    cat_file(SETNAME)
                fd = open(file, "r")
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' set-bytes".format(osd=osd, pg=pg, json=JSON)
                ret = call(cmd, shell=True, stdin=fd)
                if ret != 0:
                    print "Bad exit status {ret} from set-bytes to restore object".format(ret=ret)
                    ERRORS += 1

    try:
        os.unlink(GETNAME)
    except:
        pass
    try:
        os.unlink(TESTNAME)
    except:
        pass
    try:
        os.unlink(SETNAME)
    except:
        pass

    print "Testing list-attrs get-attr"
    ATTRFILE = r"/tmp/attrs.{pid}".format(pid=pid)
    VALFILE = r"/tmp/val.{pid}".format(pid=pid)
    for basename in db.keys():
        file = os.path.join(DATADIR, basename)
        JSON = [l for l in JSONOBJ if l.find(basename) != -1]
        JSON = JSON[0]
        for pg in OBJREPPGS:
            OSDS = get_osds(pg, OSDDIR)
            for osd in OSDS:
                DIR = os.path.join(OSDDIR, os.path.join(osd, os.path.join("current", "{pg}_head".format(pg=pg))))
                fname = [ f for f in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, f)) and string.find(f, basename + "_") == 0 ]
                if not fname:
                    continue
                fname = fname[0]
                afd = open(ATTRFILE, "w")
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' list-attrs".format(osd=osd, pg=pg, json=JSON)
                ret = call(cmd, shell=True, stdout=afd)
                afd.close()
                if ret != 0:
                    print cmd
                    print "Bad exit status {ret}".format(ret=ret)
                    ERRORS += 1
                    continue
                keys = get_lines(ATTRFILE)
                values = dict(db[basename]["xattr"])
                for key in keys:
                    if key == "_" or key == "snapset":
                        continue
                    key = key.strip("_")
                    if key not in values:
                        print "The key {key} should be present".format(key=key)
                        ERRORS += 1
                        continue
                    exp = values.pop(key)
                    vfd = open(VALFILE, "w")
                    cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal  --pgid {pg} '{json}' get-attr {key}".format(osd=osd, pg=pg, json=JSON, key="_" + key)
                    ret = call(cmd, shell=True, stdout=vfd)
                    vfd.close()
                    if ret != 0:
                        print cmd
                        print "Bad exit status {ret}".format(ret=ret)
                        ERRORS += 1
                        continue
                    lines = get_lines(VALFILE)
                    val = lines[0]
                    if exp != val:
                        print "For key {key} got value {got} instead of {expected}".format(key=key.strip("_"), got=val, expected=exp)
                        ERRORS += 1
                if len(values) != 0:
                    print "Not all keys found, remaining keys:"
                    print values

    print "Checking pg info"
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type info --pgid {pg} | grep '\"pgid\": \"{pg}\"'".format(osd=osd, pg=pg)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                print "FAILURE: getting info for pg {pg} from {osd}".format(pg=pg, osd=osd)
                ERRORS += 1

    print "Checking pg logs"
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            tmpfd = open(TMPFILE, "w")
            cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type log --pgid {pg}".format(osd=osd, pg=pg)
            ret = call(cmd, shell=True, stdout=tmpfd)
            if ret != 0:
                print "FAILURE: getting log for pg {pg} from {osd}".format(pg=pg, osd=osd)
                ERRORS += 1
            HASOBJ = pg in OBJREPPGS + OBJECPGS
            MODOBJ = False
            for line in get_lines(TMPFILE):
                if line.find("modify") != -1:
                    MODOBJ = True
                    break
            if HASOBJ != MODOBJ:
                print "FAILURE: bad log for pg {pg} from {osd}".format(pg=pg, osd=osd)
                MSG = (HASOBJ and [""] or ["NOT "])[0]
                print "Log should {msg}have a modify entry".format(msg=MSG)
                ERRORS += 1

    try:
        os.unlink(TMPFILE)
    except:
        pass

    print "Checking pg export"
    EXP_ERRORS = 0
    os.mkdir(TESTDIR)
    for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0 ]:
        os.mkdir(os.path.join(TESTDIR, osd))
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            mydir = os.path.join(TESTDIR, osd)
            fname = os.path.join(mydir, pg)
            cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type export --pgid {pg} --file {file}".format(osd=osd, pg=pg, file=fname)
            ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
            if ret != 0:
                print "FAILURE: Exporting pg {pg} on {osd}".format(pg=pg, osd=osd)
                EXP_ERRORS += 1

    ERRORS += EXP_ERRORS

    print "Checking pg removal"
    RM_ERRORS = 0
    for pg in ALLREPPGS + ALLECPGS:
        for osd in get_osds(pg, OSDDIR):
            cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type remove --pgid {pg}".format(pg=pg, osd=osd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                print "FAILURE: Removing pg {pg} on {osd}".format(pg=pg, osd=osd)
                RM_ERRORS += 1

    ERRORS += RM_ERRORS

    IMP_ERRORS = 0
    if EXP_ERRORS == 0 and RM_ERRORS == 0:
        print "Checking pg import"
        for osd in [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0 ]:
            dir = os.path.join(TESTDIR, osd)
            for pg in [ f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir,f)) ]:
                file = os.path.join(dir, pg)
                cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --file {file}".format(osd=osd, file=file)
                ret = call(cmd, shell=True, stdout=nullfd)
                if ret != 0:
                    print cmd
                    print "FAILURE: Importing from {file}".format(file=file)
                    IMP_ERRORS += 1
    else:
        print "SKIPPING IMPORT TESTS DUE TO PREVIOUS FAILURES"

    ERRORS += IMP_ERRORS
    call("/bin/rm -rf {dir}".format(dir=TESTDIR), shell=True)

    if EXP_ERRORS == 0 and RM_ERRORS == 0 and IMP_ERRORS == 0:
        print "Checking replicated import data"
        for file in [ f for f in os.listdir(DATADIR) if f.find(REP_NAME) == 0 ]:
            path = os.path.join(DATADIR, file)
            tmpfd = open(TMPFILE, "w")
            cmd = "find {dir} -name '{file}_*'".format(dir=OSDDIR, file=file)
            ret = call(cmd, shell=True, stdout=tmpfd)
            tmpfd.close()
            obj_locs = get_lines(TMPFILE)
            if len(obj_locs) == 0:
                print "Can't find imported object {name}".format(name=file)
                ERRORS += 1
            for obj_loc in obj_locs:
                cmd = "diff -q {src} {obj_loc}".format(src=path, obj_loc=obj_loc)
                ret = call(cmd, shell=True)
                if ret != 0:
                    print "FAILURE: {file} data no imported properly into {obj}".format(file=file, obj=obj_loc)
                    ERRORS += 1

        vstart(new=False)
        wait_for_health()

        print "Checking erasure coded import data"
        for file in [ f for f in os.listdir(DATADIR) if f.find(EC_NAME) == 0 ]:
            path = os.path.join(DATADIR, file)
            try:
                os.unlink(TMPFILE)
            except:
                pass
            # print "Checking {file}".format(file=file)
            cmd = "./rados -p {pool} get {file} {out}".format(pool=EC_POOL, file=file, out=TMPFILE)
            call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
            cmd = "diff -q {src} {result}".format(src=path, result=TMPFILE)
            ret = call(cmd, shell=True)
            if ret != 0:
                print "FAILURE: {file} data not imported properly".format(file=file)
                ERRORS += 1
            try:
                os.unlink(TMPFILE)
            except:
                pass

        call("./stop.sh", stderr=nullfd)
    else:
        print "SKIPPING CHECKING IMPORT DATA DUE TO PREVIOUS FAILURES"

    call("/bin/rm -rf {dir}".format(dir=DATADIR), shell=True)

    if ERRORS == 0:
        print "TEST PASSED"
        sys.exit(0)
    else:
        print "TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS)
        sys.exit(1)

if __name__ == "__main__":
    main()
