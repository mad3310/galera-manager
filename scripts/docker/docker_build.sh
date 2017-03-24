#!/bin/bash
BUILE_LOG=build.log

VERSION=`grep 'version:' ../rpm/build_rpm.yml |sed 's/version: //g'`
RELEASE=`grep 'release:' ../rpm/build_rpm.yml |sed 's/release: //g'`
MYSQL_VERSION=`grep 'mysql_ver:' ../rpm/build_rpm.yml |sed 's/mysql_ver: //g'`

FINALE_VERSION=${VERSION}'-'${RELEASE}

/bin/sed -i "s/mcluster-manager-version/mcluster-manager-${FINALE_VERSION}/g" letv_mcluster_image${MYSQL_VERSION}/Dockerfile
/bin/sed -i "s/mcluster-manager-version/mcluster-manager-${FINALE_VERSION}/g" mcluster_vip_gbalancer/Dockerfile

#echo ' -------------------start import letv-centos6 images----------------------' > ${BUILE_LOG}
#docker import http://pkg-repo.oss.letv.com/pkgs/centos6/images/letv-centos6.tar letv:centos6 >> ${BUILE_LOG}
#echo ' -------------------finish import letv-centos6 images----------------------' >> ${BUILE_LOG}

echo ' -------------------start building mcluster images----------------------' >> ${BUILE_LOG}
/usr/bin/docker build -t="120.132.124.218:5000/polex/mcluster${MYSQL_VERSION}:${FINALE_VERSION}-1" letv_mcluster_image${MYSQL_VERSION}/ >> ${BUILE_LOG}
echo ' -------------------finish building letv-mcluster images----------------------' >> ${BUILE_LOG}

echo ' -------------------start building letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}
/usr/bin/docker build -t="120.132.124.218:5000/polex/mcluster_gbalancer:${FINALE_VERSION}-1" mcluster_vip_gbalancer/  >> ${BUILE_LOG}
echo ' -------------------finish building letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}

echo ' -------------------start push letv-mcluster images----------------------' >> ${BUILE_LOG}
/usr/bin/docker push 120.132.124.218:5000/polex/mcluster${MYSQL_VERSION}:${FINALE_VERSION}-1  >> ${BUILE_LOG}
echo ' -------------------finish push letv-mcluster images----------------------' >> ${BUILE_LOG}

echo ' -------------------start push letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}
/usr/bin/docker push 120.132.124.218:5000/polex/mcluster_gbalancer:${FINALE_VERSION}-1  >> ${BUILE_LOG}
echo ' -------------------finish push letv-mcluster-gbalancer images----------------------' >> ${BUILE_LOG}

/bin/sed -i "s/mcluster-manager-${FINALE_VERSION}/mcluster-manager-version/g" letv_mcluster_image${MYSQL_VERSION}/Dockerfile
/bin/sed -i "s/mcluster-manager-${FINALE_VERSION}/mcluster-manager-version/g" mcluster_vip_gbalancer/Dockerfile
