import os

test = 2
resultFileNames = [
    'toyex_Wi_comparison',
    'testBank2000NoRandomNoise_Wi_comparison',
    'andreaHelpdesk_Wi_comparison',
]
inputColabFileNames = [
    'colab_toyex_Wi',
    'colab_testBank2000NoRandomNoise_Wi',
    'colab_andreaHelpdesk_Wi',
]
inputMainFileNames = [
    'main_toyex_Wi',
    'main_testBank2000NoRandomNoise_Wi',
    'main_andreaHelpdesk_Wi',
]

colabFile = os.getcwd() + '/colabOutput/' + inputColabFileNames[test]
mainFile = os.getcwd() + '/output/' + inputMainFileNames[test]
resultFile = os.getcwd() + '/comparisonResult/' + resultFileNames[test]

if __name__ == '__main__':

    colabSet = set((line.strip() for line in open(colabFile)))
    mainSet = set((line.strip() for line in open(mainFile)))
    noDiff = True

    with open(resultFile, 'w') as diff:

        for line in mainSet:
            if line not in colabSet:
                noDiff = False
                diff.write('[-] {}\n'.format(line))  # present in main but not in colab

        for line in colabSet:
            if line not in mainSet:
                noDiff = False
                diff.write('[+] {}\n'.format(line))  # present in colab but not in main

        if noDiff:
            diff.write('NO DIFFERENCE!')
    # with open(colabFile, 'r') as file1:
    #     with open(mainFile, 'r') as file2:
    #         diff = set(file1).difference(file2)
    #
    # diff.discard('\n')
    #
    # with open(resultFile, 'w') as file_out:
    #     if len(diff) != 0:
    #         for line in diff:
    #             file_out.write(line)
    #     else:
    #         file_out.write('NO DIFFERENCE!')
