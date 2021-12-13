def onFileFinalIG(filePath, fileName, final_df):
    # filePath with / on the end
    # file name without extension

    lines = []
    result = final_df.collect()

    for o in result:
        if o.Wi is not None:
            lines.append('trace_id: ' + str(o.trace_id))
            for v_n, v_e in o.V:
                lines.append('v ' + str(v_n) + ' ' + v_e)
            o.Wi.sort(key=lambda tup: tup[0][0], reverse=True)
            for start, end in o.Wi:
                lines.append('e ' + str(start[0]) + ' ' + str(end[0]) + ' ' + str(start[1]) + '__' + str(end[1]))
            lines.append('')

    with open(filePath + fileName + '.txt', "x") as f:
        f.write('\n'.join(lines))

    print('instance graphs are available on ' + filePath + fileName + '.txt')
