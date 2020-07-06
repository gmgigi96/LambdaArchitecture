#!/bin/sh


export AWS_DEFAULT_REGION=us-east-1

PRI_KEY=~/gianmaria_bigdata.pem

CLUSTER_ID=`aws emr create-cluster --applications Name=Hadoop Name=ZooKeeper Name=Oozie Name=Spark --ebs-root-volume-size 32 --ec2-attributes '{"KeyName":"gianmaria_bigdata","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-2773b778","EmrManagedSlaveSecurityGroup":"sg-09b529e22f880cca8","EmrManagedMasterSecurityGroup":"sg-080d8ca2f6dcd3cc5"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-6.0.0 --log-uri 's3n://gmgigi96bucket/' --name 'BigDataProj2' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":2}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":2}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1 | awk '/ClusterId/{print $2}' | sed 's/\"//g' | sed 's/\,//g'`

echo "Cluster ID = $CLUSTER_ID" 

# copia file di deploy (config, deploy.sh, src contenente gli script python)
aws emr put --cluster-id $CLUSTER_ID --key-pair-file $PRI_KEY --src "deploy-batch.sh" --dest /home/hadoop/
aws emr put --cluster-id $CLUSTER_ID --key-pair-file $PRI_KEY --src config --dest /home/hadoop/

aws emr put --cluster-id $CLUSTER_ID --key-pair-file $PRI_KEY --src src --dest /home/hadoop/

aws emr ssh --cluster-id $CLUSTER_ID --key-pair-file $PRI_KEY --command "chmod +x deploy-batch.sh"

