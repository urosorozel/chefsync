#!/usr/bin/python

"""Chef server 11 sync (script assumes that ssh key are installed for slave nodes)."""

__author__ = "Uros Orozel"
__copyright__ = "Copyright 2015, IBM"
__version__ = "1.0.0"
__maintainer__ = "Uros Orozel"
__email__ = "uroszel@au.ibm.com"
__status__ = "Production"
__doc__ = '''
    Usage:
        chefsyncnew.py master cookbook (<name> <version>)... [options]
        chefsyncnew.py master (environment|role|databag) <name>... [options]
        chefsyncnew.py master all [options] 
        chefsyncnew.py slave <slavename> [options]
        chefsyncnew.py (-h | --help | --version)

    Options:
        -h, --help  Show this screen and exit.
        --verbose   Verbose mode.
    '''

import os
import sys
import subprocess
import json
import textwrap
import logging
from docopt import docopt
import logging.handlers
import shutil
from glob import glob
def set_logging(logfile, log_level):
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    if log_level == "DEBUG":
        ch = logging.StreamHandler()
        ch.setFormatter(format)
        log.addHandler(ch)

    fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=(1048576*5), backupCount=7)
    fh.setFormatter(format)
    log.addHandler(fh)
    return log

def make_dir(dir):
    try:
        os.stat(dir)
    except:
        log.info("Creating directory %s" % dir)
        os.mkdir(dir)

def rm_dir(dir):
    try:
        log.info("Deleting directory %s" % dir)
        shutil.rmtree(dir)

    except:
        log.info("Directory %s does not exist!" % dir)

def write_knife(name):
    knife = """
    ssl_verify_mode     :verify_none
    log_level           :info
    log_location        STDOUT
    node_name           "admin"
    cookbook_path       ["{0}"]
  """.format(os.path.join(os.getcwd(), config["dirs"]["oldversions"]))

    knife = textwrap.dedent(knife)
    file = open(os.path.join(os.getcwd(), config["dirs"]["knife"], name), 'w')
    file.write(knife)
    file.close()


def run_process(cmd, working):
    logging.debug("STDOUT: from command: %s" % cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=working)
    (output, err) = p.communicate()
    p_status = p.wait()
    if output:
        pass
        #log.info("Stdout: %s" % output)
    if err:
        logging.error("Stderr: %s\n" % err)
    return output

def set_knife_options(host, keyname, knife):
    key = os.path.join(config["dirs"]["keys"], keyname)
    knife_config = os.path.join(config["dirs"]["knife"], knife)
    knife_opt = "-s {0} -k {1} -u admin -c {2}".format(host, key, knife_config)
    return knife_opt


def download_chefrepo(download_type):
    knife_opt = set_knife_options(config["master"]["chef_url"], config["master"]["keyname"], config["master"]["knife"])
    chef_repo_path = "--chef-repo-path " + os.path.join(os.getcwd(), config["dirs"]["chefrepo"])
    knife_command = "knife download /{0} {1} {2}".format(download_type, chef_repo_path, knife_opt)
    output = run_process(knife_command, ".")

def download_chef_item(download_type, item_name):
    knife_opt = set_knife_options(config["master"]["chef_url"], config["master"]["keyname"], config["master"]["knife"])
    chef_repo_path = "--chef-repo-path " + os.path.join(os.getcwd(), config["dirs"]["chefrepo"])
    if download_type == "data_bags":
        knife_command = "knife download /{0}/{1} {2} {3}".format(download_type, item_name, chef_repo_path, knife_opt)
    else:
        knife_command = "knife download /{0}/{1}.json {2} {3}".format(download_type, item_name, chef_repo_path, knife_opt)
    output = run_process(knife_command, ".")


def rsync_to_chefrepo(server, ssh_user):
    script_root = os.getcwd()

    exclude = "--delete --exclude=*%s" % config["logname"]
    rsync_command = "rsync -a {0} -e  \'ssh -o \"StrictHostKeyChecking=no\"\' {1} {2}@{3}:".format(exclude, script_root,
                                                                                                   ssh_user,
                                                                                                   server)
    run_process(rsync_command, ".")


def execute_remote_upload(server, user, slave_name):
    ssh_command = "ssh -o \"StrictHostKeyChecking=no\" {0}@{1} \"python chefsync/chefsyncnew.py slave {2} --verbose \"".format(
        user, server, slave_name)
    # print ssh_command
    run_process(ssh_command, ".")


def upload_local_repo(slave_name):
    log.info("Uploading to local chef server url: %s" % slave_name)
    slave = config["slaves"][slave_name]
    knife_opt = set_knife_options(slave["chef_url"], slave["keyname"], slave["knife"])
    for upload_type in config["types"]:
        log.info(" - Uploading: %s" % upload_type)
        chef_repo_path = "--chef-repo-path " + os.path.join(os.getcwd(), config["dirs"]["chefrepo"])
        knife_command = "knife upload /{0} {1} {2}".format(upload_type, chef_repo_path, knife_opt)
        run_process(knife_command, ".")
        

def pre_check():
    for dir in config["dirs"]:
        log.info("Checking for: %s" % dir)
        make_dir(dir)

    # Write knife configs
    try:
        os.stat(os.path.join(os.getcwd(), config["dirs"]["knife"], config["master"]["knife"]))
    except:
        log.info("Creating knife config for master site: %s" % config["master"]["site"])
        write_knife(config["master"]["knife"])

    for slave in config["slaves"]:
        slave = config["slaves"][slave]
        try:
            os.stat(os.path.join(os.getcwd(), config["dirs"]["knife"], slave["knife"]))
        except:
            log.info("Creating knife config for slave site: %s" % slave["site"])
            write_knife(slave["knife"])


def download_cookbook(bookname, version):
    make_dir(os.path.join(config["dirs"]["oldversions"], version))
    knife_sub_command = "knife cookbook download {0} {1} {2} -d %s --force" % os.path.join(config["dirs"]["oldversions"], version)
    knife_opt = set_knife_options(config["master"]["chef_url"], config["master"]["keyname"], config["master"]["knife"])
    knife_command = knife_sub_command.format(bookname, version,knife_opt)
    ver_file = open(os.path.join(config["dirs"]["oldversions"], bookname + ".txt"),'w')
    ver_file.write(bookname + " skip " + version)
    ver_file.close()
    run_process(knife_command, ".")

def get_cookbook_versions(cookbook):
    knife_sub_command = "knife cookbook show {0} {1}"
    knife_opt = set_knife_options(config["master"]["chef_url"], config["master"]["keyname"], config["master"]["knife"])
    knife_command = knife_sub_command.format(cookbook,knife_opt)
    versions = run_process(knife_command, ".")
    ver_file = open(os.path.join(config["dirs"]["oldversions"], cookbook + ".txt"),'w')
    ver_file.write(versions)
    ver_file.close()
    versions = versions.split()
    if len(versions) <= 2:
        log.info("Downloading skipped for: %s as there is only one version" % cookbook )
        return None 

    else:
        for version in versions[2:]:
            log.info("Downloading cookbook: %s %s" % (cookbook, version))
            download_cookbook(cookbook, version)

def get_latest_local_cookbooks(knife_opt):
    knife_sub_command = "knife cookbook list {0}".format(knife_opt)
    booklist = run_process(knife_sub_command, ".")
    books = get_cookbook_info(booklist)
    return books

def upload_cookbook(cookbook, version, knife_opt):
    knife_sub_command = "knife cookbook upload {0} {1} -o %s " % os.path.join(config["dirs"]["oldversions"], version)
    knife_command = knife_sub_command.format(cookbook, knife_opt)
    run_process(knife_command, ".")

def upload_local_versions(slave_name):
    slave = config["slaves"][slave_name]
    knife_opt = set_knife_options(slave["chef_url"], slave["keyname"], slave["knife"])
    cookbooks = get_latest_local_cookbooks(knife_opt)
    if os.listdir(config["dirs"]["oldversions"]):
        books = glob( config["dirs"]["oldversions"] + "/*.txt")
        
        for book in books:
            bookver = open(book, 'r').read()
            versions = bookver.split()
            cookbook = versions[0]

            if len(versions) <= 2:
                log.info("Uploading skipped for: %s as there is only one version" % cookbook )
            else:
                for version in versions[2:]:
                    log.info("Uploading cookbook: %s %s" % (cookbook, version))
                    upload_cookbook(cookbook, version, knife_opt)


def get_cookbook_info(booklist):
    books = {}
    for line in booklist.strip().split("\n"):
        key, value = line.split()
        books[key] = value
    return books


def get_latest_cookbooks():
    knife_sub_command = "knife cookbook list {0}"
    knife_opt = set_knife_options(config["master"]["chef_url"], config["master"]["keyname"], config["master"]["knife"])
    knife_command = knife_sub_command.format(knife_opt)
    booklist = run_process(knife_command, ".")
    books = get_cookbook_info(booklist)
    return books

def upload_to_slaves():
    ssh_user = config["ssh_user"]
    for cslave in config["slaves"]:
        slave = config["slaves"][cslave]
        log.info("Uploading to slave: %s" % slave["site"])
        rsync_to_chefrepo(slave["server_ip"], ssh_user)
        execute_remote_upload(slave["server_ip"], ssh_user, cslave)
       


def main(options):
    os.chdir(os.path.dirname(sys.argv[0]))
    CONFIG = "config.json"
    global config
    config = json.loads(open(CONFIG, 'r').read())
    #print sys.argv
    #print options

    # set logging
    global log
    if options["--verbose"]:
       
        log = set_logging(config["logname"], "DEBUG")
        log.info("Enabled verbose mode.")
    else:
        log = set_logging(config["logname"], "INFO")
        log.info("Enabled info logging mode.")

    if options["master"]:
        log.info("Running in master mode")
        # remove chefrepo and oldversions
        rm_dir(config["dirs"]["oldversions"])
        rm_dir(config["dirs"]["chefrepo"])
        # Pre check 
        pre_check()

        log.info("Downloading from master: %s" % config["master"]["site"])

        if options["environment"]:
            log.info("Downloading environment items")
            for item_name in options["<name>"]:
                log.info("Downloading environment: %s" % item_name)
                download_chef_item("environments", item_name)
            upload_to_slaves()

        if options["role"]:
            log.info("Downloading role items")
            for item_name in options["<name>"]:
                log.info("Downloading role: %s" % item_name)
                download_chef_item("roles", item_name)
            upload_to_slaves()

        if options["databag"]:
            log.info("Downloading data bag items")
            for item_name in options["<name>"]:
                log.info("Downloading data bag: %s" % item_name)
                download_chef_item("data_bags", item_name)
            upload_to_slaves()

        if options["cookbook"]:
            log.info("Downloading cookbook")
            for index,item_name in enumerate(options["<name>"]):
                item_version = options["<version>"][index]
                log.info("Downloading cookbook: %s %s" % (item_name, item_version))
                download_cookbook(item_name, item_version)
            upload_to_slaves()


        if options["all"]:
            for download_type in config["types"]:
                log.info(" - Downloading: %s" % download_type)
                download_chefrepo(download_type)

            logging.info(" - Downloading old versions")
            books = get_latest_cookbooks()
            print books
            for book,version in books.iteritems():
                logging.info("- Downloading cookbook: %s" % book)
                get_cookbook_versions(book)
            upload_to_slaves()

    if options["slave"]:
        log.info("Running in slave mode")
        slave_name = options["<slavename>"] 
        slave = config["slaves"][slave_name]
        knife_opt = set_knife_options(slave["chef_url"], slave["keyname"], slave["knife"])
        upload_local_repo(slave_name)
        upload_local_versions(slave_name)




if __name__ == "__main__":
    options = docopt(__doc__, version=__version__)
    main(options)
