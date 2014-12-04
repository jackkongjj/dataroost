rm -rf tmpDeploy
/c/windows/Microsoft.NET/Framework64/v4.0.30319/msbuild.exe DataRoostAPI/DataRoostAPI.csproj -p:DeployOnBuild=true -p:PublishProfile=Deployer -p:Configuration=DEV
deployer.client.exe -n=dataRoostDev -p=tmpDeploy -s=net.tcp://webdeva04.prod.factset.com -v=ERROR
rm -rf tmpDeploy
