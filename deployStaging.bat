rm -rf tmpDeploy
/c/windows/Microsoft.NET/Framework64/v4.0.30319/msbuild.exe DataRoostAPI/DataRoostAPI.csproj -p:DeployOnBuild=true -p:PublishProfile=Deployer -p:Configuration=STAGING
deployer.client.exe -n=dataRoostStaging -p=tmpDeploy -s=net.tcp://ffvwebstga01.prod.factset.com -v=ERROR
rm -rf tmpDeploy
