def timeline_vis(time_preds_multicells):
    timeline_info = time_preds_multicells
    raw_values = sorted(timeline_info.select('time_order', 'date', 'diffv1_y_score', 'accu_y_score', 'accu_y_score_m7', 'concept_name').toPandas().to_numpy().tolist(), key=lambda x: x[0])
    lab_mask = sorted(timeline_info.withColumn('m', when(col('domain_id')=='Measurement', 1).otherwise(0)).select('time_order', 'm').toPandas().to_numpy().tolist(), key=lambda x: x[0])
    data = {
        'raw_values': raw_values,
        'lab_mask': lab_mask,
        'covid_index': timeline_info.select('covid_index').distinct().collect()[0][0],
        'final_prediction': raw_values[-1][3]
    }
    print(data)

    # visualization code
    # record the prob, prob_m7 for the days with events
    accu_dict_tmp = {}
    for _, date, _, prob, prob_m7, _ in data['raw_values'][1:]:
        if (not prob) or np.isnan(prob): continue
        if (date-data['covid_index']).days not in accu_dict_tmp:
            accu_dict_tmp[(date-data['covid_index']).days] = []
        accu_dict_tmp[(date-data['covid_index']).days].append((prob, prob_m7))
    accu_dict_tmp = {k: v[-1] for k, v in accu_dict_tmp.items()}

    # build the accumulated weighted predictions for each day from days with events
    accu_dict = {}
    # find the first non zero probs
    tmp_idx = 0
    while np.isnan(data['raw_values'][tmp_idx][3]):
        tmp_idx += 1
    prob_prev = data['raw_values'][tmp_idx][3]
    prob_m7_prev = data['raw_values'][tmp_idx][4]
    for i in range(-7, 29):
        coeff = max((i+7)/35, 0)
        if i in accu_dict_tmp:
            prob_prev, prob_m7_prev = accu_dict_tmp[i]
        accu_dict[i] = coeff*prob_prev+(1-coeff)*prob_m7_prev

    # record all events that causes a perturbation in the final prediction
    diff_dict_cat = {}
    diff_dict_lab = {}
    for (_, date, prob, _, _, name), (_, is_lab) in zip(data['raw_values'], data['lab_mask']):
        if (not prob) or np.isnan(prob): continue
        if (date-data['covid_index']).days < -7:
            continue
        else:
            if not is_lab:
                if (date-data['covid_index']).days not in diff_dict_cat:
                    diff_dict_cat[(date-data['covid_index']).days] = []
                # if data['final_prediction']-prob!=0.:
                diff_dict_cat[(date-data['covid_index']).days].append((data['final_prediction']-prob, name))
            else:
                if (date-data['covid_index']).days not in diff_dict_lab:
                    diff_dict_lab[(date-data['covid_index']).days] = []
                # if data['final_prediction']-prob!=0.:
                diff_dict_lab[(date-data['covid_index']).days].append((data['final_prediction']-prob, name))

    # removes all visit events unless there is no events in the same day
    def pick_max_diff(li):
        list_without_visit = []
        for i, (val, name) in enumerate(li):
            if 'isit' not in name:
                list_without_visit.append((val, name))
        if list_without_visit:
            li = list_without_visit
        names = []
        vals = []
        max_val = li[0][0]
        vals.append(li[0][0])
        names.append(li[0][1])
        for i, (val, _) in enumerate(li):
            if abs(val) == abs(max_val):
                vals.append(val)
                names.append(li[i][1])
            elif abs(val) > abs(max_val):
                names = [li[i][1]]
                vals = [val]
        if len(set(vals))>1:
            print('vals contain positive and negative values of the same magnitude, this is not handled yet')
        return (vals[0], list(set(names)))

    diff_dict_cat = {k: pick_max_diff(v) for k, v in diff_dict_cat.items() if v}
    diff_dict_lab = {k: pick_max_diff(v) for k, v in diff_dict_lab.items() if v}
    cumu_dict = accu_dict

    cat_impact = diff_dict_cat
    lab_impact = diff_dict_lab

    x_lab = list(lab_impact.keys())
    y_lab = [i[0] for i in lab_impact.values()]
    y_lab_names = [i[1] for i in lab_impact.values()]
    x_cat = list(cat_impact.keys())
    y_cat = [i[0] for i in cat_impact.values()]
    y_cat_names = [i[1] for i in cat_impact.values()]
    x_inc = np.array(list(cumu_dict.keys()))
    y_inc = np.array(list(cumu_dict.values()))

    print(x_cat, y_cat)
    print(x_lab, y_lab)
    print(y_lab_names)

    def plot_timeline(x_cat, y_cat, x_lab, y_lab, x_inc, y_inc):
        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=False, figsize=(7, 5), dpi=300,
            gridspec_kw={'height_ratios': [2, 1]}, constrained_layout=True)

        covid_index_color = (0.8, 0.2, 0.2, 0.8)
        markerline_cat, stemlines_cat, baseline_cat = ax1.stem(x_cat, y_cat, 'b', markerfmt='bo', label='Condition occurences')
        markerline_lab, stemlines_lab, baseline_lab = ax1.stem(x_lab, y_lab, 'g', markerfmt='go', label='Measurements')
        for baseline in [baseline_lab, baseline_cat]:
            baseline.set_color('gray')
            baseline_cat.set_xdata([-7, 28])

        ymin = min(min(y_cat), min(y_lab))
        ymax = max(max(y_cat), max(y_lab))

        ax1.vlines(x=0, ymin=ymin, ymax=ymax*0.8, colors=covid_index_color, linestyles='dotted')
        ax1.set_xlim([-7, 28])
        ax1.set_xlabel('Days from COVID', horizontalalignment='right', x=1.0)
        # ax1.set_yscale('log')
        ax1.set_title('Patient timeline tracking predicted risk changes due to events')
        ax1.legend(frameon=False)

        for x, y, name in zip(x_cat+x_lab, y_cat+y_lab, y_cat_names+y_lab_names):
            y_label = y+ymax*0.03 if y > 0 else ymax*0.03
            ax1.text(x-0.2, y_label, (','.join(name))[:10], rotation=70, ha='left', fontsize=5)

        ax2.plot(x_inc, y_inc)
        ax2.fill_between(x_inc, y_inc, 0, alpha=0.2)
        ax2.vlines(x=0, ymin=0, ymax=1, colors=covid_index_color, linestyles='dotted', label='COVID index')
        ax2.set_xlim([-7, 28])
        ax2.set_ylim([0, 1])

        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        # ax1.spines['bottom'].set_visible(False)
        ax1.spines['left'].set_visible(False)

        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        # ax2.spines['bottom'].set_visible(False)
        ax2.spines['left'].set_visible(False)
        ax2.set_title('Cumulative PASC Risk')
        ax2.text(0, 1.12, 'COVID index', fontsize=10, color=covid_index_color, ha='center', va='center', rotation=0)
        ax2.text(26, y_inc[-1]+0.1, f'Prob={y_inc[-1]:.3f}', fontsize=10, color='gray', ha='center', va='center', rotation=0)

        ax1.get_xaxis().set_ticks([-7, 0, 7, 14, 21, 28])
        ax2.get_xaxis().set_ticks([])

        plt.tight_layout()
        plt.show()

    plot_timeline(x_cat, y_cat, x_lab, y_lab, x_inc, y_inc)
