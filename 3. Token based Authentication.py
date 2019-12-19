import pandas as pd
from flask import Flask, request
from flask_restplus import Resource, Api, fields, reqparse, inputs, abort
import json
from functools import wraps
from time import time
from itsdangerous import SignatureExpired, JSONWebSignatureSerializer, BadSignature

class AuthenticationToken:
    def __init__(self, secret_key, expires_in):
        self.secret_key = secret_key
        self.expires_in = expires_in
        self.serializer = JSONWebSignatureSerializer(secret_key)

    def generate_token(self, username):
        info = {
            'username': username,
            'creation_time': time()
        }


        token = self.serializer.dumps(info)
        return token.decode()

    def validate_token(self, token):
        info = self.serializer.loads(token.encode())

        if time() - info['creation_time'] > self.expires_in:
            raise SignatureExpired("Token got expired")

        return info['username']

SECRET_KEY = "Very long random string: secret key"
expires_in = 600
auth = AuthenticationToken(SECRET_KEY,expires_in)

app = Flask(__name__)
api = Api(app, authorizations={
                'API-KEY': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'AUTH-TOKEN'
                }
        },
        security='API-KEY',
        default="Books", #swagger default namespace
        title="Book Dataset", #swagger title
        description="Token Authentication on Flask-Restplus using book dataset ") #swagger description

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):

        token = request.headers.get('AUTH-TOKEN')
        print(token)
        if not token:
            abort(401, 'Authentication token is missing')

        try:
            user = auth.validate_token(token)
        except SignatureExpired as e:
            abort(401, e.message)
        except BadSignature as e:
            abort(401, e.message)
        
        return f(*args, **kwargs)

    return decorated


#Book model expected as input
book_model = api.model('Book', {
    'Flickr_URL': fields.String,
    'Publisher': fields.String,
    'Author': fields.String,
    'Title': fields.String,
    'Date_of_Publication': fields.Integer,
    'Identifier': fields.Integer,
    'Place_of_Publication': fields.String
})


credential_model = api.model('credential', {
    'username': fields.String,
    'password': fields.String
})

credential_parser = reqparse.RequestParser()
credential_parser.add_argument('username', type=str)
credential_parser.add_argument('password', type=str)

@api.route('/token')
class Token(Resource):
    @api.response(200, 'Token Generated successfully')
    @api.doc(description="Generates Authentication token")
    @api.expect(credential_parser, validate=True)
    def get(self):
        args = credential_parser.parse_args()
        username = args.get('username')
        password = args.get('password')

        if username == 'admin' and password == 'admin':
            return {"token": auth.generate_token(username)}

        return {"message": "Incorrect credentials"}, 401
        

parser = reqparse.RequestParser()
parser.add_argument('order', choices=list(column for column in book_model.keys()))
parser.add_argument('ascending', type=inputs.boolean)


@api.route('/books')
class Bookslist(Resource):

    @api.response(200, 'Sucessful')
    @api.doc(description='Get all books')
    @requires_auth
    def get(self):
        args = parser.parse_args()

        #reading the input from user
        order = args.get('order')
        ascending = args.get('ascending', True)

        if order:
            df.sort_values(by=order, inplace=True, ascending=ascending)
        
        json_str = df.to_json(orient='index')

        ds = json.loads(json_str)  # str json to real json

        final_ds = []

        for idx in ds:
            book = ds[idx]
            book['Identifier'] = int(idx)
            final_ds.append(book)

        return final_ds

    @api.response(201, 'Book created successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description= "Add a new book")
    @api.expect(book_model, validate=True)
    @requires_auth
    def post(self):
        book = request.json
        print(book['Identifier'])
        if 'Identifier' not in book:
            return {"message": "Missing Identifier"}, 400

        id = book['Identifier']

        if id in df.index:
            return{"message": "Identifier already exist in book dataset"}

        for key in book:
            if key not in book_model.keys():
                return {"message": "{} is invalid".format(key)}, 400
            df.loc[id,key] = book[key]

        return {"message": "Book {} is created".format(id)}, 201

@api.route('/books/<int:id>')
@api.param('id', 'The Book identifier')
class Books(Resource):
    @api.response(404, 'Book not found')
    @api.response(200, 'Book data retrieved Successfully')
    @api.doc(description="Get a book by its ID")
    @requires_auth
    def get(self,id):
        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))
        
        book = dict(df.loc[id])
        return book

    @api.response(404, 'Book was not found')
    @api.response(200, 'Book data deleted Successfully')
    @api.doc(description="Delete a book by its ID")
    @requires_auth
    def delete(self,id):
        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))
        df.drop(id, inplace=True)
        return {"Message": "Book {} is removed.".format(id)},200

    @api.response(404, 'Book not found')
    @api.response(400, 'Validation Error')
    @api.response(200, 'Book got updated Successfully')
    @api.expect(book_model, validate=True)
    @api.doc(description="Update a book by its ID")
    @requires_auth
    def put(self,id):
        if id not in df.index:
            api.abort(404, "Book {} doesn't exist".format(id))
        
        # get the payload and convert it into json
        book = request.json

        # Book ID cannot be changed
        if 'identifier' in book and id != book['identifier']:
            return{"Message": "identifier cannot be changed".format(id)},400

        # update the values
        for key in book:
            if key not in book_model.keys():
                #unexpected column
                return {"message" : "Identifier cannot be changed".format(id)},400
            df.loc[id,key] = book[key]

        return {"Message": "Book {} has been successfully updated.".format(id)},200

if __name__ == '__main__':
    columns_to_drop = ['Edition Statement',
                       'Corporate Author',
                       'Corporate Contributors',
                       'Former owner',
                       'Engraver',
                       'Contributors',
                       'Issuance type',
                       'Shelfmarks'
                       ]
    csv_file = "Books.csv"
    df = pd.read_csv(csv_file)

    # drop unnecessary columns
    df.drop(columns_to_drop, inplace=True, axis=1)

    # clean the date of publication & convert it to numeric data
    new_date = df['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
    new_date = pd.to_numeric(new_date)
    new_date = new_date.fillna(0)
    df['Date of Publication'] = new_date

    # replace spaces in the name of columns
    df.columns = [c.replace(' ', '_') for c in df.columns]

    # set the index column; this will help us to find books with their ids
    df.set_index('Identifier', inplace=True)

    # run the application
    app.run(debug=True)