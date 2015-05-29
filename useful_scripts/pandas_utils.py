
def stratified_corr(s1, s2, method='pearson'):
    ''' Correlation, adjusted for 
    '''
    # s2 = boolean, w very few True
    # randomly sample equal number of points for which it's false
    n_true, n_false = s2.sum(), len(s2)-s2.sum()
    n = min(n_true, n_false)
    false_inds_to_corr = list(random.sample(s1.index[~s2], n))
    true_inds_to_corr = list(random.sample(s1.index[s2], n))
    inds_to_corr = false_inds_to_corr + true_inds_to_corr
    del false_inds_to_corr, true_inds_to_corr, n_true, n_false
    return s1.ix[inds_to_corr].corr(s2.ix[inds_to_corr],method=method)


def draw_scatter_plot(dat, x, y, cluster_col='cluster_v1'):
    clusters = [c for c in list(set(dat[cluster_col])) if
        (c!=np.nan) and (isinstance(c,str) or not math.isnan(c))]
    
    sample_inds_to_use = []
    for i in clusters:
        all_inds_for_clstr = dat[dat[cluster_col]==i].index
        sample_for_clstr = random.sample(all_inds_for_clstr, 200)
        sample_inds_to_use.extend(sample_for_clstr)
    sample_inds = pd.Series([False for _ in range(len(dat))],index=dat.index)
    sample_inds[sample_inds_to_use] = True
    
    labels = dat[cluster_col]
    plt.close()
    for i, c in zip(clusters, COLORS):
        print i, c
        records = dat[sample_inds & (labels==i)]
        plt.scatter(records[x], records[y], color=c, lw=0.0)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.legend(clusters)

def get_cluster_data(dat, cluster_col='cluster'):
    cols_to_examine = [c for c in dat.columns if is_numerical(dat[c])]
    means = dat.groupby(cluster_col)[cols_to_examine].mean()
    cluster_size = dat.groupby(cluster_col)['SolarContactId'].count()
    res = pd.DataFrame({'size':cluster_size}).join(means)
    return res


