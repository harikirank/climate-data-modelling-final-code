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

