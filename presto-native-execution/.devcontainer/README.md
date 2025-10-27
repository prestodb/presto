# How to develop Presto C++ with dev-containers in CLion

> **_NOTE:_**  For this to work you need CLion 2025.2.2 or greater.

If you can't or want't to build the development environment on your machine, but still using a modern IDE "natively", you can use dev-containers. With them you can have your IDE frontend working against CLion backend running on a docker container. In order to set it up, execute

```sh
docker compose build centos-native-dependency
```
Once the image is built, open the `presto-native-execution` module on CLion. Then, right click on `.devcontainer\devcontainer.json` and in the contextual menu select `Dev Containers->Create Dev Container and mount sources...->CLion`. Wait for the container to be up and running.

The source code is mounted from your machine so any change made into it from the dev-container will also be on your machine.

## Debug or execute `presto_server`

Reload CMake project and configure the `presto_server` executable as defined [here](https://github.com/mblanco-denodo/presto/tree/master/presto-native-execution). Compile the project as you need. Then, execute the script ./devcontainer/install-shared-libs.sh inside the container. This will create a directory /runtime-libraries and copy all the shared libraries needed for your compilation runtime in there. Edit the `presto_server` configuration to add the environment variable `LD_LIBRARY_PATH=/runtime-libraries`. This way, you'll have the same environment as distributed prestissimo images.

## Known errors
 - In some cases an error such as `Computing backend... error. Collection contains no element matching the predicate` can appear. The feature is still in beta. In this case, the container will be created and running, but might have been an issue starting the CLion backend inside the container. To resolve this issue, close CLion, reopen it and in the `Welcome to CLion` window go to `Remote Development (beta)->Dev Containers`. You should see that the container `Presto C++ Dev Container` is up and running, so connect to it. In this case, the backend should start properly and the project should be opened.

 - In some cases you might to manually add the mounted repo to the trusted directories for the dev-container
    ```sh
    git config --global --add safe.directory /workspace/presto
    ```
