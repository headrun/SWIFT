from flask import Flask, flash, render_template, request, redirect

app = Flask(__name__, static_url_path="/static", static_folder="static")
app.secret_key = '_5#y2L"F4Q8z\n\xec]/'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

@app.route('/upload', methods=('GET','POST'))
def upload():
    if request.method == 'POST':
        try:
            filename = 'Impendi_Analytics_Masterfile_Dataset.xlsx'
            file_ = request.files.get('xlsx-file', '')
            file_.save(filename)
            flash ('File uploaded successfully', 'success')

        except:
            flash('Encountered error while uploading the file', 'error')

        return render_template('upload.html')
    else:
    	return render_template('upload.html')
