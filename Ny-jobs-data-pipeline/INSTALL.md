# Installation Instructions
 

**IMPORTANT**: *Any installation requires at least 8Gb of RAM for proper operation.*

1. Install Docker Desktop

	  **Using Windows**: Install docker desktop https://docs.docker.com/desktop/install/windows-install/
	
	  **Using MacOS**: Follow instructions in the below link to setup your MacOS https://docs.docker.com/desktop/install/mac-install/ 
	  
2. Ensure Docker Desktop is up and running

3. Spin-up Spark cluster: 
	
	a. Open shell (**Windows**: Windows Powershell/ **MacOS**: bash or any available shell)
	
	b. Change directory to the /project1
		
		cd {project1-directory}
	
	c. Docker compose
	
		docker compose -f ./docker-compose.yml --project-name my_assesment up

### Validation

1. Open Docker desktop and check for **my_assesment** project having 5 containers as below

	  ![docker-desktop](https://github.com/projectforyou/project1/blob/main/pictures/docker-desktop-containers.png)
	
2. Spark Cluster is up and running. Go to http://localhost:8080/. You should get a Spark Master UI with 2 worker nodes -
	
   	![spark-master-ui](https://github.com/projectforyou/project1/blob/main/pictures/spark-master-ui.png)

