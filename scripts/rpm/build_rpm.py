#!/usr/bin/env python2.6
# encoding: utf-8

import os

from fabric.api import local, lcd
from fabric.colors import yellow, green
import yaml


def download_source():
    print(yellow("Start downloading source code ..."))
    local("git clone -b {0} {1} {2}".format(
        y['git_branch'], y['git_repo'], project_root))
    print(green("Download has been completed!"))


def build_source():
    print(yellow("Start building the source code..."))
    with lcd(project_root):
        code_path = "/opt/letv/{0}".format(y['name'])
        local("mkdir -p build{0}".format(code_path))
        with lcd('src'):
            local("python2.6 -m compileall .")
            local('find . -name "*.py"  | xargs rm -f')
            local("cp -R * ../build/{0}".format(code_path))
        local("mv etc build")
    print(green("Building completed!"))


def generate_rpm():
    print(yellow("Start generate rpm package ..."))
    commad = """fpm -f \
        -s {source_type} \
        -t {target_type} \
        -C {buildroot} \
        -n {name} \
        -v {version} \
        -d "{dependencies}" \
        -m "{maintainor}" \
        -p {packout} \
        --category "{category}" \
        --description "{description}" \
        --url "{url}" \
        --vendor "{vendor}" \
        --before-install {before_install} \
        --after-install {after_install} \
        --after-remove {after_remove} \
        --iteration {release} \
        --verbose""".format(
        source_type=y['source_type'],
        target_type=y['target_type'],
        buildroot=os.path.join(project_root, 'build'),
        name=y['name'],
        version=y['version'],
        dependencies=', '.join(y['dependencies']),
        maintainor=y['maintainor'],
        packout=y['packout'],
        category=y['category'],
        description=y['description'],
        url=y['url'],
        vendor=y['vendor'],
        before_install=os.path.join(project_root, y['before_install']),
        after_install=os.path.join(project_root, y['after_install']),
        after_remove=os.path.join(project_root, y['after_remove']),
        release=y['release']
    )
    if not os.path.exists(y['packout']):
        local("sudo mkdir -p {0}".format(y['packout']))
    local(commad)
    print(green("Generate rpm package Successful!"))


def upload_rpm():
    pass


def clean():
    local("rm -rf {0}".format(project_root))


if __name__ == "__main__":
    y = yaml.safe_load(open('build_rpm.yml'))
    project_root = '/tmp/{0}'.format(y['name'])
    clean()
    download_source()
    build_source()
    generate_rpm()
    #  upload_rpm()
    clean()
