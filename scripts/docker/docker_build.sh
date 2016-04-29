#!/bin/bash
BUILE_LOG=build.log
VERSION=`/bin/cat version.cnf`

echo ' -------------------start import letv-centos6 images----------------------' > ${BUILE_LOG}
docker import http://pkg-repo.oss.letv.com/pkgs/centos6/images/letv-centos6.tar letv:centos6 >> ${BUILE_LOG}
echo ' -------------------finish import letv-centos6 images----------------------' >> ${BUILE_LOG}

echo ' -------------------start building letv-mcluster images----------------------' >> ${BUILE_LOG}
/usr/bin/docker build -t="10.160.140.32:5000/letv-mcluster:${VERSION}" letv_mcluster_image/ >> ${BUILE_LOG}
echo ' -------------------finish building letv-mcluster images----------------------' >> ${BUILE_LOG}

echo ' -------------------start building letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}
/usr/bin/docker build -t="10.160.140.32:5002/letv_mcluster_gbalancer:${VERSION}" mcluster_vip_gbalancer/  >> ${BUILE_LOG}
echo ' -------------------start building letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}

echo ' -------------------start push letv-mcluster images----------------------' >> ${BUILE_LOG}
/usr/bin/docker push 10.160.140.32:5002/letv-mcluster:${VERSION}  >> ${BUILE_LOG}
echo ' -------------------finish push letv-mcluster images----------------------' >> ${BUILE_LOG}

echo ' -------------------start push letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}
/usr/bin/docker push 10.160.140.32:5002/letv_mcluster_gbalancer:${VERSION}  >> ${BUILE_LOG}
echo ' -------------------finish push letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}
