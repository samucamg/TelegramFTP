# Telegram-FTP

Use o espaço ilimitado do telegram como backup via ftp.

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

  1. Crie um novo bot em [BotFather](https://telegram.dog/botfather).
  2. Obtenha o API_ID e API_HASH em [my.telegram.org](https://my.telegram.org).
  3. Create uma database mongo em [MongoDB Cloud](https://cloud.mongodb.com/) (ou use seu servidor) e copie a string de conexão.
  4. Coloque todas as variáveis em na raiz do bot no arquivo .env
  5. Adicione o bot ao seu canal com direito de administrador.
  6. Execute o arquivo 'python3 get_channel_id.py`, envie o comando `/id` no seu canal para obter o id do canal.
  7. Copie o ID para .env
  8. Crie uma database mongodb com o nome `ftp`.
  9. Execute 'python3 setup_database.py`.
  10. Execute 'python3 accounts_manager.py` para criar sua conta ftp.
  11. Execute `main.py`.

</details>
