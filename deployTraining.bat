rm -rf tmpDeploy
/c/windows/Microsoft.NET/Framework64/v4.0.30319/msbuild.exe DataRoostAPI/DataRoostAPI.csproj -p:DeployOnBuild=true -p:PublishProfile=Deployer -p:Configuration=Training
deployer.client.exe -n=dataRoostTraining -p=tmpDeploy -s=net.tcp://cieffwebstga01.prod.factset.com -v=ERROR
rm -rf tmpDeploy
