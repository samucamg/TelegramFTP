# SamucaFTP
Utilize o espaço ilimitado do telegram para guardar todos seus arquivos em um chat, e acessar por ftp.

Use o espaço ilimitado do telegram como backup via ftp.

Antes de qualquer coisa, você precisa de um servidor ou vps linux, caso não tenha, pode instalar o wsl 2 no seu computador windows, e instalar o sistema operacional ubuntu 20.04 ou ubuntu 22.04.  Você pode ver como fazer no tutorial do site da Microssoft sobre [Como Instalar o Wsl2] (https://learn.microsoft.com/pt-br/windows/wsl/install)

Você também irá encontrar vários tutoriais no youtube ensinando como habilitar o wsl 2 e instalar o ubuntu no seu servidor windows no [youtube.] (https://www.youtube.com/results?search_query=Como+instalar+o+wsl2+no+windows&sp=CAASAhAB)

Após instalar o Wsl2 no seu computador, baixe o zip desse repositório em releases descompacte abra a página onde o mesmo se encontra, e digite "bash" como
mostrado nas figuras abaixo, para acessar o ambiente linux:



![SamucaFtp](https://i.imgur.com/OB9RKOJ.jpg)














![SamucaFtp bash](https://i.imgur.com/PNNrmwA.jpg)

<details>
<summary><b>Você necessita configurar as variáveis abaixo:</b></summary>

`API_ID`: Acesse [my.telegram.org](https://my.telegram.org) para obter o seu.

`API_HASH`: Acesse [my.telegram.org](https://my.telegram.org) para obter o seu.

`BOT_TOKEN`: Crie um novo bot utilizando [BotFather](https://telegram.dog/botfather).

`MONGODB`: Crie um DB e obtenha o link de conexão em [mongodb.com] (https://www.mongodb.com/)

`CHAT_ID`: Id do Chat para onde serão enviados os arquivos.

`HOST`: Host do FTP deixe como padrão (Padrão: 0.0.0.0).

`PORT`: Porta do servidor FTP (Padrão: 9021).

</details>

<details>
<summary><b>Setup:</b></summary>
Antes de iniciar o setup, verifique se você tem o python3 instalado, ou instale utilizando o comando abaixo:
sudo apt update && sudo apt install python3-pip -y
A seguir:

  1. Crie um novo bot em [BotFather](https://telegram.dog/botfather).
  2. Obtenha o API_ID e API_HASH em [my.telegram.org](https://my.telegram.org).
  3. Crie um banco de dados Mongo DB com o nome de ftp [MongoDB Cloud](https://cloud.mongodb.com/) (ou use seu servidor) e copie a string de conexão.
Aprenda aqui, [Como Criar gratuitamente sua base de dados Mongo DB] (https://www.youtube.com/watch?v=6b3YH0kK3ig)
  Caso pretenda utilizar uma quantidade muito grande de arquivos, é preferível criar o seu próprio banco de dados Mongo-db veja o tutorial sobre [Como instalar e Criar sua base de dados Mongo DB no ubuntu 20.04] (https://www.digitalocean.com/community/tutorials/how-to-install-mongodb-on-ubuntu-20-04-pt)
  4. Coloque todas as variáveis em na raiz do bot no arquivo .env
  5. Adicione o bot ao seu canal com direito de administrador.
  6. Execute o arquivo 'python3 get_channel_id.py`, envie o comando `/id` no seu canal para obter o id do canal.
  7. Copie o ID para .env
  8. Execute 'python3 setup_database.py`.
  9. Execute 'python3 accounts_manager.py` para criar sua conta ftp.
  10. Execute `main.py`.

</details>
<summary>Aconselho a utilização de Uma VPS ou windows com wsl2 com <b>Ubuntu 22.04</b></summary>
