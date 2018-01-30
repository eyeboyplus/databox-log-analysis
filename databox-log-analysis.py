from flask import Flask, request, jsonify

from core import LogCaseEn
from core import LogAnalysis
from core import Log

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/123.html')
def test():
    return '123'


@app.route('/init')
def init():
    # print('init')
    # logCase = LogCaseEn.LogCase('LogCase')
    # logCase.clear()
    # logCase.conf_data_initial()
    log_analysis = LogAnalysis.LogAnalysis()
    log_analysis.init()
    return jsonify(
        status='success'
    )

@app.route('/service', methods=['POST'])
def service():
    # print('service')
    json = request.get_json()
    print(json)
    # log_analysis = LogAnalysis.LogAnalysis()
    # log = Log.Log(str(json))
    # res = log_analysis.analysis(log)
    # flag = res['status']
    # print(res)
    logCase = LogCaseEn.LogCase('LogCase')
    logCase.mix_control(str(json))
    # if flag:
    #     return jsonify(
    #         status='pass',
    #         info=res['info']
    #     )
    # else:
    #     return jsonify(
    #         status='deny',
    #         info=res['info']
    #     )

if __name__ == '__main__':
    app.run()