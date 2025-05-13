=======
DBeaver
=======

`DBeaver <https://dbeaver.io/>`_ is a multiplatform open source database tool. Follow these steps to configure DBeaver to query Presto.

Install and Run DBeaver
=======================

Download and install DBeaver from `DBeaver Download <https://dbeaver.io/download/>`_, then launch DBeaver.

Configure DBeaver
=================

1. In DBeaver, select **Database** > **New Database Connection**. 

2. Select **PrestoDB**, then **Next**. If the **Main** tab is not selected, select it. 

3. If the Presto server is not running locally, select **URL** and edit the default value in **JDBC URL**. 

4. In **Username**, enter a user name - the default is `admin`. 

5. Select **Test Connection**. 

   Note: You may be prompted to download a driver. If needed, follow the prompts in the popup **Driver settings** window to do so. 

   When successful, a **Connection test** window is displayed that shows "Connected".

6. Select **Finish**. 