import os
test = 2
resultFileNames = [
    'toyex_W_comparison',
    'testBank2000NoRandomNoise_W_comparison',
    'andreaHelpdesk_W_comparison',
]
inputColabFileNames = [
        'colab_toyex_W',
        'colab_testBank2000NoRandomNoise_W',
        'colab_andreaHelpdesk_W',
        ]
inputMainFileNames = [
    'main_toyex_W',
    'main_testBank2000NoRandomNoise_W',
    'main_andreaHelpdesk_W',
]

colabFile = os.getcwd() + '/colabOutput/' + inputColabFileNames[test]
mainFile = os.getcwd() + '/output/' + inputMainFileNames[test]
resultFile = os.getcwd() + '/comparisonResult/' + resultFileNames[test]

if __name__ == '__main__':
    with open(colabFile, 'r') as file1:
        with open(mainFile, 'r') as file2:
            diff = set(file1).difference(file2)

    diff.discard('\n')

    with open(resultFile, 'w') as file_out:
        if len(diff) != 0:
            for line in diff:
                file_out.write(line)
        else:
            file_out.write('NO DIFFERENCE!')