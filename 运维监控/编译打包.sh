--------------------------------------------------------- 前端 -----------------------------------------------------------------------
--1.1 设置下载全局包地址
npm root -g 
npm config set prefix "C:\nodejs\node_global"

npm config set prefix "C:\nodejs\node_cache"

--设置源
npm config set registry https://registry.npm.taobao.org

 --nrm是npm registry 管理工具，可以切换你想要的源
  sudo npm install nrm

npm i

npm run buid


nrm use taobao

npm install -g cnpm --registry=https://registry.npm.taobao.org

cnpm cache clean --force

sudo cnpm install

sudo cnpm run build

------------------------------------------------------- java  -----------------------------------------------------------------------
mvn clean package -DskipTests -Prelease
