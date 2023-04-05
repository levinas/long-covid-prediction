def fig_feature_category_ablation(feature_category_ablation_results):
    df = feature_category_ablation_results.toPandas()
    df['exp'] = df['exp_name'].str.replace('superset', 'All')
    df['exp'] = df['exp'].str.replace(' - ', ' minus ')

    plt.style.use('default')
    plt.style.use('seaborn-deep')

    COLOR = (0.35, 0.35, 0.35)
    mpl.rcParams['text.color'] = COLOR
    mpl.rcParams['axes.labelcolor'] = COLOR
    mpl.rcParams['xtick.color'] = COLOR
    mpl.rcParams['ytick.color'] = COLOR

    # plot with error bars
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(10, 8), dpi=300,
        gridspec_kw={'height_ratios': [1.15, 1]})

    df1 = df.loc[df['exp_name'].str.contains('superset')].sort_values(by=['acc_mean'])
    df2 = df.loc[~df['exp_name'].str.contains('superset')].sort_values(by=['acc_mean'])
    
    height = 0.7
    alpha = 1
    color = (46/255, 114/255, 221/255)
    ax1.barh(y=df1['exp'], width=df1['acc_mean'], color=color, height=height, alpha=alpha)
    ax1.set_xlabel('Accuracy')
    ax1.set_xlim([0.5, 1])
    for i, v in enumerate(df1['acc_mean']):
        ax1.text(v-0.1, i-0.1,f'{v:.4f}', color='white', fontweight='bold')
    # ax1.xaxis.set_visible(False)
    ax1.xaxis.set_ticks([0.5, 1])
    ax1.xaxis.set_ticks_position('top')
    ax1.xaxis.set_label_position('top')

    ax2.barh(y=df1['exp'], width=df1['auc_mean'], color=color, height=height, alpha=alpha)
    ax2.set_xlabel('AUROC')
    ax2.set_xlim([0.5, 1])
    ax2.set_yticks([])
    for i, v in enumerate(df1['auc_mean']):
        ax2.text(v-0.1, i-0.1,f'{v:.4f}', color='white', fontweight='bold')
    # ax2.xaxis.set_visible(False)
    ax2.xaxis.set_ticks([0.5, 1])
    ax2.xaxis.set_ticks_position('top')
    ax2.xaxis.set_label_position('top')

    ax3.barh(y=df2['exp']+' only', width=df2['acc_mean'], height=height)
    ax3.set_xlabel('Accuracy')
    ax3.set_xlim([0.5, 1])
    for i, v in enumerate(df2['acc_mean']):
        ax3.text(v-0.1, i-0.1,f'{v:.4f}', color='white', fontweight='bold')

    ax4.barh(y=df2['exp']+' only', width=df2['auc_mean'], height=height)
    ax4.set_xlabel('AUROC')
    ax4.set_xlim([0.5, 1])
    ax4.set_yticks([])
    for i, v in enumerate(df2['auc_mean']):
        if v > 0.6:
            ax4.text(v-0.1, i-0.1,f'{v:.4f}', color='white', fontweight='bold')
        else:
            ax4.text(v+0.02, i-0.1, f'{v:.4f}', color=(0.298,0.447,0.69), fontweight='bold')

    for ax in (ax1, ax2, ax3, ax4):
        ax.tick_params(color=COLOR, labelcolor=COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(COLOR)

    plt.tight_layout()
    plt.show()