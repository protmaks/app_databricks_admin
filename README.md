# Databricks Apps Configuration (Admin Guide)

Currently, installation is only supported from Git. The app will be added to the Databricks Marketplace later.

## 1 - Install App from Git
Your administrator must enable Git-backed deployments in your Databricks Apps Workspace to allow deployments directly from repositories.

1. Go to **Settings** -> **Workspace Settings**.
2. Navigate to the **Previews** section.
3. Enable the toggle for **Databricks Apps Git-backed deployments**.

![Enable Git-backed deployments](assets/apps_enable_install_from_git.png)

After your administrator has enabled Git-backed deployments, follow these steps to deploy an application directly from GitHub:

1. In the sidebar, navigate to **Compute** and select the **Apps** tab.
2. Click the **Create app** button in the top right corner.
3. In the creation dialog, select **Git repository** as the source.
4. Fill in the repository details:
    *   **Git repo URL:** Enter the full URL of your GitHub repository.
    *   **Git provider:** Select **GitHub**.
    *   **Branch:** Specify the branch to deploy (e.g., `main`).
    *   **App source code path:** Provide the path to the folder containing your code (leave empty if the code is in the root directory).
5. Click **Create**. 

Databricks will automatically create a Service Principal for the app and begin the build process.


## 2. Grants for using App
The application can be used by both business users and administrators, depending on access settings.

### 2.1 - for Admins (Granting Administrative Access)
If the application needs to manage workspace resources (such as clusters or jobs), you must add the App's Service Principal to the Admin group.

1. Go to **Settings** -> **Identity and access** -> **Groups**.
2. Select the **admins** group.
3. Click **Add members**.
4. Search for your **App Name** (or its Service Principal ID) and add it to the group.

> **Note:** Every Databricks App creates its own identity. Granting this identity Admin rights gives the code within the app full control over the workspace.


### 2.2 - for Bisness Users (User identity for Databricks Apps)
By default, Databricks Apps run using a dedicated Service Principal. To allow apps to run under the identity of the user accessing them (Run as viewer), this feature must be enabled in the Workspace settings.

1. Go to **Settings** -> **Workspace Settings**.
2. Navigate to the **Previews** section.
3. Find and enable the toggle for **User identity for Databricks Apps**.
   * *Note: This allows developers to select "User Identity" in the app configuration.*

![Enable User identity for Databricks Apps](assets/apps_enable_user_auth.png)


## 3. Open App
1. In the sidebar, click on **Compute**.
2. Go to the **Apps** tab.
3. Click on the **Name** of the application you want to open.
4. Click to the Deploy and chouse the **main** branch.
5. Click the **Open app** button in the top right corner.

![Enable User identity for Databricks Apps](assets/app_link.png)


### 4. Resource Management

Set up access to the application
1. In the sidebar, click on **Compute**.
2. Go to the **Apps** tab.
3. Click on the **Name** of the application you want to open.
4. Click to the **Permissions** tab.
5. Set up acces.


### 5. Daily work

**Important:** Do not forget to manually stop the application or set up a scheduled job to stop it; otherwise, it will run 24/7 and continuously consume compute resources.

To stop an app:
1. Go to the **Apps** tab.
2. Select your application.
3. Click the **Stop** button in the top right corner.





I_PROMISE_I_DIDNT_PRE_CODE_THIS