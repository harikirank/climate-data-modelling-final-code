### To Create a Virtual Environment Run these commands in the linux shell
1. `python3 -m venv venv`

### To Activate the virtual environment
2. `source venv/bin/activate`

### To install the required dependencies
3. `pip install -r requirements/requirements.txt`


---
## To create a virtual machine in GCP:
1. Go to [GCP Console](https://console.cloud.google.com/)
2. Click on the hamburger menu on the left
3. Go to compute engine -> VM Instances
4. Click on create instance at the top
5. Select the configuration of the machine that you want. GPUs are not available in free trial. So, if you are on free trial please don't choose GPUs.
6. Click on Create at the bottom of the page
7. Wait for a few minutes so that the instance will be created

---
## To run the virtual machine
1. Go to [GCP Console](https://console.cloud.google.com/)
2. Click on the hamburger menu on the left
3. Go to compute engine -> VM Instances
4. You will see the VM instance that you have created in the previous step
5. Select that VM and click on start/resume at the top
6. Wait for the machine to boot up
7. Once the machine is booted up, under connect there will be SSH option. Click on that SSH option
8. A new window will open. Wait for a few seconds and you will see the linux shell for the Virtual machine
9. Now you can start doing your work on the VM
