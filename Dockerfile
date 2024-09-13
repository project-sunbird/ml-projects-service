FROM node:20

WORKDIR /opt/projects

#copy package.json file
COPY package.json /opt/projects

#install node packges
RUN npm install

#copy all files 
COPY . /opt/projects

#expose the application port
EXPOSE 3000

#start the application
CMD node app.js
