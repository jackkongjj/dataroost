rm -rf tmpDeploy
/c/windows/Microsoft.NET/Framework64/v4.0.30319/msbuild.exe DataRoostAPI/DataRoostAPI.csproj -p:DeployOnBuild=true -p:PublishProfile=Deployer -p:Configuration=PROD
deployer.client.exe -n=dataRoostProduction -p=tmpDeploy -s=net.tcp://ffvweba04.prod.factset.com -v=ERROR
deployer.client.exe -n=dataRoostProduction -p=tmpDeploy -s=net.tcp://ffvwebb04.prod.factset.com -v=ERROR
rm -rf tmpDeploy
