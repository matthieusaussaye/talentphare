
import copy

FRACDIFF = '_FRACDIFF_'
FD = '_FRACDIFF_'


def sanity_check(dict_map):
    for key_ in list(dict_map.keys()):
        if not dict_map[key_]:
            # print(f'No data for key {key_}')
            dict_map.pop(key_)
    return dict_map


# VERIF MECE
def verif_MECE(featdict, list_distinct_feat):
    featdict_all_values = [el2 for el1 in list(featdict.keys()) for el2 in featdict[el1]]
    featdict_all_values_distinct = list(set(featdict_all_values))
    # ======================================
    # CHECK CE (COLLECTIVELY EXHAUSTIVE)
    list_is_not_included = [el  for el in list_distinct_feat if (el not in featdict_all_values)]
    if list_is_not_included:
        bool_CE = False
    else:
        bool_CE = True
    # ======================================
    # CHECK ME (MUTUALLY EXCLUSIVE)
    # FIND THE INTERSECTIONS
    dict_opposite_map = {}
    for feat in featdict_all_values_distinct:
        dict_opposite_map[feat] = []
        for group in list(featdict.keys()):
            if feat in featdict[group]:
                dict_opposite_map[feat] = dict_opposite_map[feat] + [group]
    dict_intersection = {}
    for feat in list(dict_opposite_map.keys()):
        if len(dict_opposite_map[feat])>1:
            dict_intersection[feat] = dict_opposite_map[feat]
    if dict_intersection:
        bool_ME = False
    else:
        bool_ME = True
    bool_MECE = bool_CE & bool_ME
    return bool_MECE, list_is_not_included, dict_intersection


# CREATE LVL 3 Without FD
def create_subgroup_feat_dict_shap_simple(lfall, *rm):
    if rm:
        rm = rm[0]
    else:
        rm = False
    # RM WEATHER
    list_weather = [el for el in lfall if 'weather_' in el]
    list_weather_0 = [el for el in list_weather if
                      ('_ma' not in el.replace('max', '').replace('mars', '').replace('madr', ''))
                      & ('_lin' not in el) & ('_ewma' not in el)]
    list_weather_hist = [el for el in list_weather if el not in list_weather_0]
    # RM LNGFLOW
    list_lngflow = [el for el in lfall if ('LNGFLOW_' in el) & ('intake_probabilized' not in el)]
    list_lngflow_0 = [el for el in list_lngflow if
                      ('_ma' not in el.replace('max', '')) & ('_lin' not in el) & ('_ewma' not in el)
                      & ('_CONG' not in el) & ('_ST' not in el) & ('_LT' not in el) & ('_SPEC' not in el)
                      & ('_CNN' not in el)]
    list_lngflow_hist = [el for el in list_lngflow if el not in list_lngflow_0]
    # RM LNGFLOW CNN
    list_lngflow_CNN = [el for el in lfall if ('LNGFLOW_' in el) & ('intake_probabilized' in el)]
    list_lngflow_CNN_0 = [el for el in list_lngflow_CNN if ('_L' not in el)]
    list_lngflow_CNN_hist = [el for el in list_lngflow_CNN if ('_L' in el)]
    # RM TTF-JKM Spread
    list_TTFJKM = [el for el in lfall if ('JKMTTF_Spread' in el)]
    list_TTFJKM_0 = [el for el in list_TTFJKM if ('masub' not in el)]
    list_TTFJKM_hist = [el for el in list_TTFJKM if (el not in list_TTFJKM_0)]
    # RM CORR
    list_corr = [el for el in lfall if ('spearman' in el) | ('pearson' in el) | ('kendall' in el)]
    list_corr_0 = [el for el in list_corr if '_L' not in el]
    list_corr_hist = [el for el in list_corr if el not in list_corr_0]
    # RM MAPERC
    list_maperc = [el for el in lfall if 'maperc' in el]
    # RM SHOCKS
    list_shocks = [el for el in lfall if 'shocks' in el]
    # RM CRUDE FLOW
    list_crudeflow = [el for el in lfall if ('CRUDEFLOW_' in el)]
    list_crudeflow_hist = [el for el in list_crudeflow if ('madiv' in el) | ('linsub' in el)]
    list_crudeflow_0 = [el for el in list_crudeflow if el not in list_crudeflow_hist]

    # RM CRUDE OFF STORAGE
    list_crudeoffstorage = [el for el in lfall if ('CRUDE_OFFSHORE_STORAGE_' in el)]
    list_crudeoffstorage_hist = [el for el in list_crudeoffstorage if ('masub' in el) | ('linsub' in el)]
    list_crudeoffstorage_0 = [el for el in list_crudeoffstorage if el not in list_crudeoffstorage_hist]

    # RM COAL FLOW
    list_coalflow = [el for el in lfall if ('COALFLOW_' in el)]
    list_coalflow_hist = [el for el in list_coalflow if ('madiv' in el) | ('linsub' in el)]
    list_coalflow_0 = [el for el in list_coalflow if el not in list_coalflow_hist]
    # ==================
    list_rm = list_weather + list_lngflow + list_corr + list_lngflow_CNN + list_TTFJKM + list_maperc + list_shocks + \
              list_crudeflow + list_crudeoffstorage + list_coalflow
    # ==================
    lfall = [el for el in lfall if el not in list_rm]
    list_lag_past = [el for el in lfall if ('_L' in el) & ('_L-' not in el)]
    list_hist_op = [el for el in lfall if ('_ma' in el.replace('max', '')) | ('_lin' in el) | ('_ewma' in el)]
    list_vol = [el for el in lfall if ('_z' in el.replace('ztp', '').replace('zee', ''))]
    list_VLT = [el for el in lfall if ('360VLT' in el)]
    list_dyn = list(set(list_lag_past + list_hist_op))
    list_hist = list(set(list_hist_op + list_lag_past + list_vol))
    list_0 = [el for el in lfall if (el not in list_hist + list_VLT)]
    featdict = {
        # DA ========================================================
        # DA 0 P
        'dayahead_p_de': [el for el in list_0 if "da_de_p" in el],
        'dayahead_p_be': [el for el in list_0 if "da_be_p" in el],
        'dayahead_p_fr': [el for el in list_0 if "da_fr_p" in el],
        'dayahead_p_ch': [el for el in list_0 if "da_ch_p" in el],
        # DA 0 V
        'dayahead_v_de': [el for el in list_0 if "da_de_v" in el],
        'dayahead_v_be': [el for el in list_0 if "da_be_v" in el],
        'dayahead_v_fr': [el for el in list_0 if "da_fr_v" in el],
        'dayahead_v_ch': [el for el in list_0 if "da_ch_v" in el],
        # DA L P
        'dayahead_p_de_F': [el for el in list_hist if "da_de_p" in el],
        'dayahead_p_be_F': [el for el in list_hist if "da_be_p" in el],
        'dayahead_p_fr_F': [el for el in list_hist if "da_fr_p" in el],
        'dayahead_p_ch_F': [el for el in list_hist if ("da_ch_p" in el) & (("masub" in el) | ("madiv" in el))],
        'dayahead_p_ch_F0': [el for el in list_hist if ("da_ch_p" in el) & ("masub" not in el) & ("madiv" not in el)],
        # DA NS
        'dayahead_p_nl': [el for el in list_0 if "da_nl_v" in el],
        'dayahead_p_it': [el for el in list_0 if "da_it_v" in el],
        'dayahead_p_es': [el for el in list_0 if "da_es_v" in el],
        'dayahead_p_cz': [el for el in list_0 if "da_cz_v" in el],
        # IMBA ======================================================
        # IMBA 0
        'imbalance_be': [el for el in list_0 if "imbalance_be" in el],
        'imbalance_fr': [el for el in list_0 if "imbalance_fr" in el],
        # IMBA L
        'imbalance_be_F0': [el for el in list_hist if "imbalance_be" in el],
        'imbalance_fr_F0': [el for el in list_hist if "imbalance_fr" in el],
        # CO2 =======================================================
        # AUC 0 & DYN & MA_Y
        'euaauc_0': [el for el in list_0 if ('t3pa' in el) & ( 'eua_Y1_roll' not in el)],
        'euaauc_L': [el for el in list_hist if ('t3pa' in el) & ('eua_Y1_roll' not in el)],
        # EUA 0 & DYN & VOL & MACD
        'euaroll_0': [el for el in list_0 if ('eua_Y1_roll' in el) & ('a_sol_' not in el) & ('MACD' not in el)],
        'euaroll_L': [el for el in list_dyn if  ('eua_Y1_roll' in el) & ('a_sol_' not in el) & ('MACD' not in el)],
        'euaroll_VOL': [el for el in list_vol if  ('eua_Y1_roll' in el) & ('a_sol_' not in el) & ('MACD' not in el)],
        'euaroll_MACD': [el for el in lfall if ('eua_Y1_roll' in el) & ('MACD' in el)],
        # EUA SOLL CSPI
        'euasol_0': [el for el in lfall if (('a_sol_m' in el) | ('a_sol_q' in el)) & ('eua_Y1_roll' not in el)],
        'euasol_delta': [el for el in lfall if (('a_sol_m' in el) | ('a_sol_q' in el)) & ('eua_Y1_roll' in el)],
        # COAL ========================================================
        # API2 COST
        'coal_0': [el for el in list_0 if ('coal_ene' in el) & ('gas_ene' not in el) & ('sM' not in el)],
        'coal_L': [el for el in list_hist if ('coal_ene' in el) & ('gas_ene' not in el) & ('sM' not in el)],
        # CLEAN DARK SPREAD 0 DYN VOL
        'coal_sp_0': [el for el in list_0 if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) | ('dark_spread' in el)],
        'coal_sp_LY': [el for el in list_dyn if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('350' in el) | ('_LY' in el))],
        'coal_sp_LM': [el for el in list_dyn if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & (('30' in el) | ('25' in el) | ('_LM' in el))],
        'coal_sp_LW': [el for el in list_dyn if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & (
                               'masub3' not in el) & ('ma3' not in el) & ('_LM' not in el) & (
                               ('7' in el) | ('5' in el) | ('_LW' in el))],
        'coal_sp_LD': [el for el in list_dyn if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & (
                               '_LM' not in el) & (('masub3' in el) | ('ma3' in el))],
        'coal_sp_VOL_W': [el for el in list_vol if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('_z7' in el) | ('_z5' in el) | ('_zW' in el))],
        'coal_sp_VOL_M': [el for el in list_vol if (('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('30' in el) | ('25' in el) | ('_zM' in el))],
        'coal_sp_LY_S': [el for el in list_dyn if ((('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('dark_spread' in el)) & (('350' in el) | ('_LY' in el))],
        'coal_sp_LM_S': [el for el in list_dyn if ((('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('dark_spread' in el)) & ('350' not in el) & ('_LY' not in el) & (
                                 ('30' in el) | ('25' in el) | ('_LM' in el))],
        'coal_sp_LW_S': [el for el in list_dyn if (('dark_spread' in el) & ('_LW' in el))],
        'coal_sp_LD_S': [el for el in list_dyn if ((('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('dark_spread' in el)) & ('350' not in el) & ('_LY' not in el) & (
                                 '30' not in el) & ('25' not in el) & ('_LM' not in el) & (('masub3' in el) | ('ma3' in el))],
        'coal_sp_VOL_W_S': [el for el in list_vol if ((('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('dark_spread' in el)) & (('_z7' in el) | ('_z5' in el) | ('_zW' in el))],
        'coal_sp_VOL_M_S': [el for el in list_vol if ((('coal_ene' in el) & ('gas_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('dark_spread' in el)) & (('30' in el) | ('25' in el) | ('_zM' in el))],
        # GAS TTF ========================================================
        # TTF COST 0 DYN & Q
        'gas_0': [el for el in list_0 if
                  ('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & ('sM' not in el) & (
                          'MACD' not in el)],
        'gas_L': [el for el in list_hist if
                  ('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & ('sM' not in el) & (
                          'MACD' not in el)],
        'gasQ': [el for el in lfall if ('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' in el)],
        # CLEAN SPARK SPREAD
        'gas_sp_0': [el for el in list_0 if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) | ('spark_spread' in el)],
        'gas_sp_LY': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('350' in el) | ('_LY' in el))],
        'gas_sp_LM': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & (('30' in el) | ('25' in el) | ('_LM' in el))],
        'gas_sp_LW': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & (
                              '_LM' not in el) & ('masub3' not in el) & ('ma3' not in el) & (
                              ('7' in el) | ('5' in el) | ('_LW' in el))],
        'gas_sp_LD': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & (
                              '_LM' not in el) & (('masub3' in el) | ('ma3' in el))],
        'gas_sp_VOL_W': [el for el in list_vol if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('_z7' in el) | ('_z5' in el) | ('_zW' in el))],
        'gas_sp_VOL_M': [el for el in list_vol if (('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' in el)) & (('30' in el) | ('25' in el) | ('_zM' in el))],
        'gas_sp_LY_S': [el for el in list_dyn if ((('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('spark_spread' in el)) & (('350' in el) | ('_LY' in el))],
        'gas_sp_LM_S': [el for el in list_dyn if ((('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('spark_spread' in el)) & ('350' not in el) & ('_LY' not in el) & (
                                ('30' in el) | ('25' in el) | ('_LM' in el))],
        'gas_sp_LW_S': [el for el in list_dyn if (('spark_spread' in el) & ('_LW' in el))],
        'gas_sp_LD_S': [el for el in list_dyn if (('spark_spread' in el) & ('_LD' in el))],
        'gas_sp_VOL_W_S': [el for el in list_vol if ((('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('spark_spread' in el)) & (('_z7' in el) | ('_z5' in el) | ('_zW' in el))],
        'gas_sp_VOL_M_S': [el for el in list_vol if ((('gas_ene' in el) & ('coal_ene' not in el) & ('_Q' not in el) & (
                'sM' not in el)) | ('spark_spread' in el)) & (('30' in el) | ('25' in el) | ('_zM' in el))],
        'gas_MACD': [el for el in lfall if ('gas_ene' in el) & ('coal_ene' not in el) & ('sM' not in el) & (
                'MACD' in el) & ('MACDlin' not in el)],
        'gas_MACDlin': [el for el in lfall if ('gas_ene' in el) & ('coal_ene' not in el) & ('sM' not in el) & (
                'MACDlin' in el)],
        # GAS 55 TTF =========================================================
        'gas55_0': [el for el in list_0 if ('cost_gas55_energy' in el) & ('sM' not in el)],
        'gas55_DYN': [el for el in list_hist if ('cost_gas55_energy' in el) & ('sM' not in el)],
        'gas55_sp_0': [el for el in list_0 if ('cost_gas55_energy' in el) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LM': [el for el in list_hist if
                        (('cost_gas55_energy' in el) & ('L25A' in el)) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LW': [el for el in list_hist if
                        (('cost_gas55_energy' in el) & ('L3A' in el)) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LD': [el for el in list_hist if
                        (('cost_gas55_energy' in el) & ('L1A' in el)) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LM_S': [el for el in list_hist if
                          (('spark55' in el) & ('LM' in el)) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LW_S': [el for el in list_hist if
                          (('spark55' in el) & ('LW' in el)) & (('sM' in el) | ('spark55' in el))],
        'gas55_sp_LD_S': [el for el in list_hist if
                          (('spark55' in el) & ('LD' in el)) & (('sM' in el) | ('spark55' in el))],
        # GAS TTF OTHER
        'gas_TTFJKM_0' : [el for el in list_TTFJKM_0],
        'gas_TTFJKM_dyn' : [el for el in list_TTFJKM_hist],
        # GAS NO TTF  ========================================================
        # GAS UK
        'gas_nbp_0': [el for el in list_0 if 'cost_gas_nbp_energy' in el],
        'gas_nbp_dyn': [el for el in list_hist if 'cost_gas_nbp_energy' in el],
        # GAS UK
        'gas_zee_0': [el for el in list_0 if 'cost_gas_zee_energy' in el],
        'gas_zee_dyn': [el for el in list_hist if 'cost_gas_zee_energy' in el],
        # GAS UK
        'gas_iga_0': [el for el in list_0 if 'cost_gas_iga_energy' in el],
        'gas_iga_dyn': [el for el in list_hist if 'cost_gas_iga_energy' in el],
        # GAS UK
        'gas_ger_0': [el for el in list_0 if 'cost_gas_ger_energy' in el],
        'gas_ger_dyn': [el for el in list_hist if 'cost_gas_ger_energy' in el],
        # GAS JKM
        'gasjkm': list(set([el for el in lfall for el2 in ['cost_JKM_energy_m1'] if el2 in el])),
        # OIL ================================================================
        # BRENT
        'brent_0': [el for el in list_0 if 'brent_m1_roll' in el],
        'brent_dyn': [el for el in list_hist if 'brent_m1_roll' in el],
        # CSPI ================================================================
        'cspi_0': [el for el in list_0 if
                   ((('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el)) | ('cspi' in el))],
        'cspiQ': [el for el in lfall if ('gas_ene' in el) & ('coal_ene' in el) & ('_Q' in el)],
        'cspi_LY': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el))
                    & (('350' in el) | ('_LY' in el))],
        'cspi_LM': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el))
                    & ('350' not in el) & ('_LY' not in el) & (
                            ('30' in el) | ('25' in el) | ('_L20' in el) | ('_LM' in el))],
        'cspi_LW': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el))
                    & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & (
                            'masub3' not in el) & ('ma3' not in el) & ('_LM' not in el)
                    & (('7' in el) | ('3' in el) | ('_LW' in el) | ('_L5' in el))],
        'cspi_LD': [el for el in list_dyn if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el))
                    & ('350' not in el) & ('_LY' not in el) & ('30' not in el) & ('25' not in el) & ('_LM' not in el)
                    & (('masub3' in el) | ('ma3' in el))],
        'cspi_VOL_W': [el for el in list_vol if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el)) & (
                ('_z7' in el) | ('_z5' in el) | ('_zW' in el))],
        'cspi_VOL_M': [el for el in list_vol if (('gas_ene' in el) & ('coal_ene' in el) & ('_Q' not in el)) & (
                ('30' in el) | ('25' in el) | ('_zM' in el))],
        # CORRELATION TO FUNDAMENTAL ============================================
        'corr_fund_ST': [el for el in list_corr_0 if ('an5' in el) | ('on5' in el) | ('ll5' in el)],
        'corr_fund_MT': [el for el in list_corr_0 if ('an10' in el) | ('on10' in el) | ('ll10' in el)],
        'corr_fund_LT': [el for el in list_corr_0 if ('an20' in el) | ('on20' in el) | ('ll20' in el)],
        'corr_fund_ST_LW': [el for el in list_corr_hist if ('an5' in el) | ('on5' in el) | ('ll5' in el)],
        'corr_fund_MT_LW': [el for el in list_corr_hist if ('an10' in el) | ('on10' in el) | ('ll10' in el)],
        'corr_fund_LT_LW': [el for el in list_corr_hist if ('an20' in el) | ('on20' in el) | ('ll20' in el)],
        # GAS STORAGE ===========================================================
        # gas 0
        'gas_sto_i': [el for el in list_0 if ('store_' in el) & ('_lng' not in el)],
        # gas L
        'gas_sto_hst': [el for el in list_hist if ('store_' in el) & ('lng' not in el) & ('360' not in el)],
        'gas_sto_hlt': [el for el in list_hist if ('store_' in el) & ('lng' not in el) & ('360' in el)
                        & ((('_full_' not in el) & (('gasInStorage' not in el) | ('_injection_' in el)))
                           | ('_L5S' in el))],
        'gas_sto_hlt_sub': [el for el in list_hist if ('store_' in el) & ('lng' not in el) & ('360' in el)
                            & (('gasInStorage' in el)|('_full_' in el)) & ('_L5S' not in el) & ('_sub' in el)
                            & ('_injection_' not in el)],
        'gas_sto_hlt_P': [el for el in list_hist if ('store_' in el) & ('lng' not in el) & ('360' in el)
                          & (('gasInStorage' in el)|('_full_' in el)) & ('_L5S' not in el) & ('_sub' not in el)
                          & ('_injection_' not in el)],
        'gas_sto_vlt': [el for el in list_VLT if ('store_' in el) & ('lng' not in el)],
        # LNG STORAGE ===========================================================
        # lng 0
        'lngstorei': [el for el in list_0 if ('store_' in el) & ('lng' in el) & ('sendOut' not in el)],
        'lngstorei_so': [el for el in list_0 if ('store_' in el) & ('lng' in el) & ('sendOut' in el)],
        # lng L
        'lngstorehlt': [el for el in list_hist if ('store_' in el) & ('lng' in el) & ('360' in el)
                        & (('lng_sendOut' not in el) | ('_L5S' in el))],
        'lngstorehlt_so': [el for el in list_hist if ('store_' in el) & ('lng' in el) & ('360' in el)
                           & ('lng_sendOut' in el) & ('_L5S' not in el)],
        'lngstorehst': [el for el in list_hist if ('store_' in el) & ('lng' in el) & ('360' not in el)],
        'lngstorehvlt': [el for el in list_VLT if ('store_' in el) & ('lng' in el)],
        # LNG FLOW ===========================================================
        'lngflow_D1_nst': [el for el in list_lngflow_0 if (('LNGFLOW_E_AS_D1' in el) | ('LNGFLOW_EU_D1' in el))],
        'lngflow_D': [el for el in list_lngflow_0 if ('_D' in el) & ('W_E_AS_D1' not in el) & ('W_EU_D1' not in el)],
        'lngflow_W_EU': [el for el in list_lngflow_0 if ('_W' in el) & ('W_EU' in el)],
        'lngflow_W_EEU': [el for el in list_lngflow_0 if ('_W' in el) & ('W_E_EU' in el)],
        'lngflow_W_EAS': [el for el in list_lngflow_0 if ('_W' in el) & ('W_E_AS' in el)],
        'lngflow_W_SEAS': [el for el in list_lngflow_0 if ('_W' in el) & ('W_SE_AS' in el)],
        'lngflow_CONG': [el for el in list_lngflow_hist if ('_CONG' in el)],
        'lngflow_ST_EU': [el for el in list_lngflow_hist if ('_ST' in el) & ('W_EU' in el)],
        'lngflow_ST_EEU': [el for el in list_lngflow_hist if ('_ST' in el) & ('W_E_EU' in el)],
        'lngflow_ST_EAS': [el for el in list_lngflow_hist if ('_ST' in el) & ('W_E_AS' in el)],
        'lngflow_ST_SEAS': [el for el in list_lngflow_hist if ('_ST' in el) & ('W_SE_AS' in el)],
        'lngflow_LT_EU': [el for el in list_lngflow_hist if ('_LT' in el) & ('W_EU' in el)],
        'lngflow_LT_EEU': [el for el in list_lngflow_hist if ('_LT' in el) & ('W_E_EU' in el)],
        'lngflow_LT_EAS': [el for el in list_lngflow_hist if ('_LT' in el) & ('W_E_AS' in el)],
        'lngflow_LT_SEAS': [el for el in list_lngflow_hist if ('_LT' in el) & ('W_SE_AS' in el)],
        'lngflow_LM': [el for el in list_lngflow_hist if ('_madiv_x_M' in el) | ('_madiv7' in el)],
        'lngflow_LY': [el for el in list_lngflow_hist if ('_linsub_x_Y' in el) & ('W_EU' not in el)],
        'lngflow_LY_EU': [el for el in list_lngflow_hist if ('_linsub_x_Y' in el) & ('W_EU' in el)],
        'lngflow_CNN_L0_FST': [el for el in list_lngflow_CNN_0 if int(el.split('_')[-2])< 10],
        'lngflow_CNN_L0_FLT': [el for el in list_lngflow_CNN_0 if int(el.split('_')[-2])>=10],
        'lngflow_CNN_LST_FST': [el for el in list_lngflow_CNN_hist if
                                (int(el.split('_')[-3]) < 10) & (int(el.split('_')[-1][1:]) < 10)],
        'lngflow_CNN_LST_FLT': [el for el in list_lngflow_CNN_hist if
                                (int(el.split('_')[-3]) < 10) & (int(el.split('_')[-1][1:]) >= 10)],
        'lngflow_CNN_LLT_FST': [el for el in list_lngflow_CNN_hist if
                                (int(el.split('_')[-3]) >= 10) & (int(el.split('_')[-1][1:]) < 10)],
        'lngflow_CNN_LLT_FLT': [el for el in list_lngflow_CNN_hist if
                                (int(el.split('_')[-3]) >= 10) & (int(el.split('_')[-1][1:]) >= 10)],
        # COAL FLOW ===========================================================
        'coalflow_D': [el for el in list_coalflow_0 if ('_D' in el)],
        'coalflow_W': [el for el in list_coalflow_0 if ('_W' in el)],
        'coalflow_LW': [el for el in list_coalflow_hist if ('_madiv7' in el)],
        'coalflow_LM': [el for el in list_coalflow_hist if ('_madiv_x_M' in el)],
        'coalflow_LY': [el for el in list_coalflow_hist if ('_linsub_x_Y' in el)],

        # CRUDE FLOW ===========================================================
        'crudeflow_D': [el for el in list_crudeflow_0 if ('_D' in el)],
        'crudeflow_W_EU': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_EU' in el)],
        'crudeflow_W_EEU': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_E_EU' in el)],
        'crudeflow_W_EAS': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_E_AS' in el)],
        'crudeflow_W_SEAS': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_SE_AS' in el)],
        'crudeflow_W_NE_AM': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_NE_AM' in el)],
        'crudeflow_W_NW_AM': [el for el in list_crudeflow_0 if ('_W' in el) & ('W_NW_AM' in el)],
        'crudeflow_LW': [el for el in list_crudeflow_hist if ('_madiv7' in el)],
        'crudeflow_LM': [el for el in list_crudeflow_hist if ('_madiv_x_M' in el)],
        'crudeflow_LY': [el for el in list_crudeflow_hist if ('_linsub_x_Y' in el)],

        # CRUDE STORAGE ========================================================
        'crude_offstorage_0' : [el for el in list_crudeoffstorage_0 if ('ARAB' not in el)],
        'crude_offstorage_LW' : [el for el in list_crudeoffstorage_hist if ('_masub7' in el) & ('ARAB' not in el)],
        'crude_offstorage_LM' : [el for el in list_crudeoffstorage_hist if ('_masub_x_M' in el) & ('ARAB' not in el)],
        'crude_offstorage_LY' : [el for el in list_crudeoffstorage_hist if ('_linsub_x_Y' in el) & ('ARAB' not in el)],
        'crude_offstorage_AG_0' : [el for el in list_crudeoffstorage_0 if ('ARAB' in el)],
        'crude_offstorage_AG_LW' : [el for el in list_crudeoffstorage_hist if ('_masub7' in el) & ('ARAB' in el)],
        'crude_offstorage_AG_LM' : [el for el in list_crudeoffstorage_hist if ('_masub_x_M' in el) & ('ARAB' in el)],
        'crude_offstorage_AG_LY' : [el for el in list_crudeoffstorage_hist if ('_linsub_x_Y' in el) & ('ARAB' in el)],

        # FINANCE ===========================================
        # OHLC
        's': [el for el in lfall if el in ['s']],
        's' + FD: list(set([el for el in lfall for el2 in ['s' + FD,] if el2 in el])),
        'ohlc': [el for el in lfall if el in ['o', 'c', 'l', 'h']],
        'ohlc' + FD: list(set([el for el in lfall for el2 in ['o', 'c', 'l', 'h'] if el2+FD in el[:len(FD)+1]])),
        'ohlcL': list(set([el for el in lfall for el2 in ['s_L2', 's_L1', 'o_L1', 'c_L1', 'l_L1', 'h_L1'] if el2 in el])),
        # sY_A
        'POWER_FY': [el for el in lfall if el in ['sY_A', 'sY_S', 'sY_P',]],
        'POWER_FY' + FD: list(set([el for el in lfall for el2 in ['sY_A', 'sY_S', 'sY_P',] if el2 + FD in el])),
        'POWER_FY_MA': [el for el in lfall if el in ['sY_A_ma365', 'sY_A_ma30', 'sY_A_ma7']],
        'POWER_FY_MA'+ FD: list(set([el for el in lfall for el2 in ['sY_A_ma365', 'sY_A_ma30', 'sY_A_ma7'] if el2 + FD in el])),
        'POWER_FY_DYN_LY': [el for el in lfall if ("sY_A_ma" in el) & ('365' in el) & (('sub' in el) | ('div' in el))
                            & (el not in ['sY_A_madiv365', 'sY_A_masub365', 'sY_A_ma30_ma365_sub',
                                          'sY_A_ma30_ma365_div']) & (FD not in el)],
        'POWER_FY_DYN_LY_nst': [el for el in lfall if
                                el in ['sY_A_madiv365', 'sY_A_masub365', 'sY_A_ma30_ma365_sub', 'sY_A_ma30_ma365_div']],
        'POWER_FY_DYN_LY_nst' + FD: list(set([el for el in lfall for el2 in  ['sY_A_madiv365', 'sY_A_masub365',
                                                                              'sY_A_ma30_ma365_sub', 'sY_A_ma30_ma365_div'] if el2 + FD in el])),
        'POWER_FY_DYN_LM': [el for el in lfall if
                            ("sY_A_ma" in el) & ('30' in el) & ('365' not in el) & (('sub' in el) | ('div' in el))],
        'POWER_FY_DYN_LW': [el for el in lfall if
                            ("sY_A_ma" in el) & (('5' in el) | ('7' in el)) & ('365' not in el) & ('30' not in el) & (
                                    ('sub' in el) | ('div' in el))],
        # sM_A
        'POWER_FQ': [el for el in lfall if el in ['sQ_A', 'sQ_S', 'sQ_P',]],
        'POWER_FQ' + FD: list(set([el for el in lfall for el2 in ['sQ_A', 'sQ_S', 'sQ_P',] if el2+FD in el])),
        # sM_A
        'POWER_FM': [el for el in lfall if el in ['sM_A', 'sM_S', 'sM_P', ]],
        'POWER_FM' + FD: list(set([el for el in lfall for el2 in ['sM_A', 'sM_S', 'sM_P'] if el2 + FD in el])),
        'POWER_FM_MA': [el for el in lfall if el in ['sM_A_ma365', 'sM_A_ma30', 'sM_A_ma7', ]],
        'POWER_FM_MA'+ FD: list(set([el for el in lfall for el2 in ['sM_A_ma365','sM_A_ma30','sM_A_ma7']
                                     if el2 + FD in el])),
        'POWER_FM_DYN_LY': [el for el in lfall if ("sM_A_ma" in el) & ('365' in el) & (('sub' in el) | ('div' in el))
                            & (el not in ['sM_A_madiv365', 'sM_A_masub365', 'sM_A_ma30_ma365_sub',
                                          'sM_A_ma30_ma365_div']) & (FD not in el)],
        'POWER_FM_DYN_LY_nst': [el for el in lfall if
                                el in ['sM_A_madiv365', 'sM_A_masub365', 'sM_A_ma30_ma365_sub', 'sM_A_ma30_ma365_div']],
        'POWER_FM_DYN_LY_nst' + FD: list(set([el for el in lfall for el2 in ['sM_A_madiv365', 'sM_A_masub365',
                                                                             'sM_A_ma30_ma365_sub', 'sM_A_ma30_ma365_div'] if el2 + FD in el])),
        'POWER_FM_DYN_LM': [el for el in lfall if
                            ("sM_A_ma" in el) & ('30' in el) & ('365' not in el) & (('sub' in el) | ('div' in el))],
        'POWER_FM_DYN_LW': [el for el in lfall if
                            ("sM_A_ma" in el) & (('5' in el) | ('7' in el)) & ('365' not in el) & ('30' not in el) & (
                                    ('sub' in el) | ('div' in el))],
        # Technical
        'slowstoch': [el for el in lfall if ("SLOWSTOCH_Kline" in el)],
        'rsi': [el for el in lfall if ("RSI_" in el)],
        'macd': [el for el in lfall if
                 (("MACD_" in el) & ('lin' not in el) & ('gas_ene' not in el) & ('eua_Y1_roll' not in el))],
        'macd_lin': [el for el in lfall if
                     (("MACD" in el) & ('lin' in el) & ('gas_ene' not in el) & ('eua_Y1_roll' not in el))],
        # FWC
        'fwc': [el for el in lfall if el in ['sY1', 'sY2', 'sQ1', 'sQ2', 'sQ3', 'sM1', 'sM2', 'sM3']],
        'fwc' + FRACDIFF: list(set([el for el in lfall for el2 in
                                    [el3+ FRACDIFF for el3 in ['sY1', 'sY2', 'sQ1', 'sQ2', 'sQ3', 'sM1', 'sM2', 'sM3']]
                                    if el2 in el])),
        'fwc_dyn': [el for el in lfall if el in ['sY1_L1P', 'sY1_L2P', 'sM1_L1P', 'sM1_L2P']],
        # MT DYN
        # percentile
        'maperc_LT': [el for el in list_maperc if (('120' in el) | ('240' in el))],
        'maperc_ST': [el for el in list_maperc if (('120' not in el) & ('240' not in el) & ('mapercalh' not in el))],
        'maperclh_ST': [el for el in list_maperc if (('120' not in el) & ('240' not in el) & ('mapercalh' in el))],
        # shocks
        'rshocks' : [el for el in list_shocks if ('_adj' not in el) & ('rshocks' in el)],
        'reshocks' : [el for el in list_shocks if ('_adj' not in el) & ('reshocks' in el)],
        'rshocks_adj' : [el for el in list_shocks if ('_adj' in el) & ('rshocks' in el)],
        'reshocks_adj' : [el for el in list_shocks if ('_adj' in el) & ('reshocks' in el)],
        # dynamic Ratios
        'slt_dyn': [el for el in lfall if ((('s_lin' in el) | ('s_ma' in el)) & ('cspi' not in el) & ('_P' in el))],
        # Z
        'vol': [el for el in list_vol if ('s_z' in el) & ('_div' not in el)],
        'vol_p': [el for el in list_vol if ('s_z' in el) & ('_div' in el)],
        # Rank
        'rank_ST': [el for el in lfall if (('s_rank' in el) & ('60' not in el))],
        'rank_LT': [el for el in lfall if (('s_rank' in el) & ('60' in el))],
        # ca + intra
        'sintra': [el for el in lfall if el in ['bs_L-1A']],
        'sintra'+FD: [el for el in lfall if 'bs_L-1A' + FD in el],
        'sca': list(set([el for el in lfall for el2 in ['caD', 'caP', 'caBB', 'intra_delta', 'intra_ratio',] if
                         el2 in el])),
        # WEATHER ===================================================
        # week
        'sW': [el for el in lfall if (('W1_A' in el) & ('W1_A_z' not in el))],
        'sW_z': [el for el in lfall if ('W1_A_z' in el)],
        # weather_0
        'weather0_cloud': [el for el in list_weather if (el in list_weather_0) & ('cloud' in el)],
        'weather0_temp': [el for el in list_weather if (el in list_weather_0) & ('temp' in el)],
        'weather0_wind': [el for el in list_weather if (el in list_weather_0) & ('wind' in el)],
        # weather M
        'weather_FM_MA_cloud': [el for el in list_weather_hist if ('Y30' not in el) & ('Y12' not in el) & ('cloud' in el)],
        'weather_FM_MA_temp': [el for el in list_weather_hist if ('Y30' not in el) & ('Y12' not in el) & ('temp' in el)],
        'weather_FM_MA_wind': [el for el in list_weather_hist if ('Y30' not in el) & ('Y12' not in el) & ('wind' in el)],
        # weather Y
        'weather_FM_LY_cloud': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('cloud' in el)],
        'weather_FM_LY_wind': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('wind' in el)],
        # weather per CO
        'weather_FM_LY_temp_de': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('berlin' in el)|('hamburg' in el)|('munich' in el)|('cologne' in el))],
        'weather_FM_LY_temp_fr': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('paris' in el)|('lyon' in el)|('marseille-13e' in el)|('strasbourg' in el))],
        'weather_FM_LY_temp_it': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('rome' in el)|('milan' in el)|('naples' in el))],
        'weather_FM_LY_temp_po': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('warsaw' in el)|('katowice' in el)|('gdansk' in el))],
        'weather_FM_LY_temp_jp': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('tokyo' in el)|('yokohama' in el))],
        'weather_FM_LY_temp_us': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('new york' in el)|('los angeles' in el))],
        'weather_FM_LY_temp_au': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('sydney' in el))],
        'weather_FM_LY_temp_nb': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('brussel' in el) | ('amsterdam' in el))],
        'weather_FM_LY_temp_ch': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('zurich' in el))],
        'weather_FM_LY_temp_es': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('madrid' in el))],
        'weather_FM_LY_temp_ce': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('vienna' in el)|('prague' in el)|('budapest' in el))],
        'weather_FM_LY_temp_med': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                   & (('zagreb' in el)|('athens' in el))],
        'weather_FM_LY_temp_ee': [el for el in list_weather_hist if (('Y30' in el) | ('Y12' in el)) & ('temp' in el)
                                  & (('kiev' in el)|('vilnius' in el)|('minsk' in el))],
        # WEATHER ===================================================
        # Size prod + matu
        'sizematu': [el for el in lfall if el in ['day2del', 'nh', 'nh_day2del_div', ]],
        # CO
        'country': [el for el in lfall if el in ['is_DE', 'is_F7', 'is_FD', ]],
        'base': [el for el in lfall if el in ['base']],
        # SEASON
        'season': [el for el in lfall if el in ['t_EOY']],
    }
    # Remove Null Keys
    if rm:
        featdict = sanity_check(featdict)
    bool_MECE, list_is_not_included, dict_intersection = verif_MECE(featdict, lfall)
    if not bool_MECE:
        if list_is_not_included:
            print(f'MISSING FEATURE IN MAPPING TO SUBGROUP_lvl3: {list_is_not_included}')
            print('FIX : ADD FEAT AS ITS OWN GROUP')
            for el in list_is_not_included:
                featdict[el] = [el]
        if dict_intersection:
            print(f'FEATURE OVERLAP IN MAPPING TO SUBGROUP_lvl3: {dict_intersection}')
            print(f'FIX : KEEP ONLY FIRST GROUP')
            for feat_ in list(dict_intersection.keys()):
                for group_ in dict_intersection[feat_][1:]:
                    featdict[group_].remove(feat_)
                    print(f'RM {feat_} FROM {group_}')
    return featdict


# CREATE LVL 3 With FD
def create_subgroup_feat_dict_shap(lfall):
    lfall_no_FD = [el for el in lfall if FRACDIFF not in el]
    lfall_with_FD = [el for el in lfall if FRACDIFF in el]
    featdict = create_subgroup_feat_dict_shap_simple(lfall_no_FD, True)
    keys_no_FD = list(featdict.keys())
    for key_ in keys_no_FD:
        if FRACDIFF in key_:
            del featdict[key_]
    featdict_FD = create_subgroup_feat_dict_shap_simple(lfall_with_FD, True)
    # ADD FRACDIFF at the end of each group key
    keys_FD = list(featdict_FD.keys())
    for key_ in keys_FD:
        if FRACDIFF not in key_:
            featdict_FD[key_ + FRACDIFF] = featdict_FD[key_]
            del featdict_FD[key_]
    featdict.update(featdict_FD)
    # CHECK MECE FOR MERGING
    bool_MECE, list_is_not_included, dict_intersection = verif_MECE(featdict, lfall)
    if not bool_MECE:
        if list_is_not_included:
            print(f'MISSING FEATURE IN MAPPING TO SUBGROUP_lvl3: {list_is_not_included}')
            print('FIX : ADD FEAT AS ITS OWN GROUP')
            for el in list_is_not_included:
                featdict[el] = [el]
        if dict_intersection:
            print(f'FEATURE OVERLAP IN MAPPING TO SUBGROUP_lvl3: {dict_intersection}')
            print(f'FIX : KEEP ONLY FIRST GROUP')
            for feat_ in list(dict_intersection.keys()):
                for group_ in dict_intersection[feat_][1:]:
                    featdict[group_].remove(feat_)
                    print(f'RM {feat_} FROM {group_}')
    return featdict


# CREATE LVL 1, 2, 3
def create_subgroup_feat_dict_shap_big(lfall):
    # =======================================================
    # MAP LEVEL 3 ===========================================
    map_group_lvl3 = create_subgroup_feat_dict_shap(lfall)
    # =======================================================
    # MAP LEVEL 2 ===========================================
    map_group_lvl2 = copy.deepcopy(map_group_lvl3)
    list_group = list(map_group_lvl2.keys())
    # UNDERLYINGS ============================================
    # GATHER DA P MAIN
    name = 'DA'
    list_no_FD = ['dayahead_p_de', 'dayahead_p_fr', 'dayahead_p_ch', 'dayahead_p_be', 'dayahead_p_nl', 'dayahead_p_it',
                  'dayahead_p_es', 'dayahead_p_cz', 'dayahead_v_de', 'dayahead_v_fr', 'dayahead_v_ch', 'dayahead_v_be',
                  'dayahead_p_de_F', 'dayahead_p_fr_F', 'dayahead_p_ch_F', 'dayahead_p_be_F', 'dayahead_p_ch_F0',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER IMBALANCE
    name = 'IMBALANCE'
    list_no_FD = ['imbalance_be', 'imbalance_fr', 'imbalance_be_F0', 'imbalance_fr_F0',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # CO2 ================================================
    # GATHER EUA_DYN
    name = 'EUA_0'
    list_no_FD = ['euaauc_0', 'euaroll_0', 'euasol_0',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER EUA_DYN
    name = 'EUA_DYN'
    list_no_FD = ['euaauc_L', 'euaroll_L', 'euaroll_DYN', 'euaroll_MACD', 'euasol_delta', 'euaroll_VOL']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # COAL ==============================================
    # GATHER API2
    name = 'API2_DARK_CLEAN'
    list_no_FD = ['coal_0', 'coal_sp_0', 'coalQ']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER API2 LW
    name = 'API2_DARK_CLEAN_DYN_VOL_ST'
    list_no_FD =  ['coal_L', 'coal_sp_LW', 'coal_sp_LD', 'coal_sp_VOL_W', 'coal_sp_LW_S', 'coal_sp_LD_S',
                   'coal_sp_VOL_W_S',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER API2 LM
    name = 'API2_DARK_CLEAN_DYN_VOL_LT'
    list_no_FD = ['coal_sp_LM', 'coal_sp_VOL_M', 'coal_sp_LM_S', 'coal_sp_VOL_M_S', 'coal_sp_LY', 'coal_sp_LY_S',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GAS ===============================================
    # GATHER TTF
    name = 'TTF_SPARK_CLEAN'
    list_no_FD = ['gas_0', 'gas_sp_0', 'gasQ', 'gas55_0', 'gas55_sp_0']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER TTF LW
    name = 'TTF_SPARK_CLEAN_DYN_VOL_ST'
    list_no_FD = ['gas_L', 'gas_sp_LD', 'gas_sp_LW', 'gas_sp_VOL_W', 'gas_sp_LD_S', 'gas_sp_LW_S', 'gas_sp_VOL_W_S',
                  'gas55_DYN', 'gas55_sp_LW', 'gas55_sp_LD', 'gas55_sp_LW_S', 'gas55_sp_LD_S']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER TTF LM
    name = 'TTF_SPARK_CLEAN_DYN_VOL_LT'
    list_no_FD = ['gas_sp_LM', 'gas_sp_VOL_M', 'gas_sp_LM_S', 'gas_sp_VOL_M_S', 'gas_sp_LY', 'gas_sp_LY_S',
                  'gas55_sp_LM', 'gas55_sp_LM_S', 'gas_MACD', 'gas_MACDlin',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'GAS_TTFJKM'
    list_no_FD = ['gas_TTFJKM_0', 'gas_TTFJKM_dyn',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER OTHER GAS
    name = 'GAS_NO_TTF'
    list_no_FD = ['gas_nbp_0', 'gas_nbp_dyn', 'gas_zee_0', 'gas_zee_dyn',
                  'gas_iga_0', 'gas_iga_dyn', 'gas_ger_0', 'gas_ger_dyn']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # OIL
    name = 'OIL'
    list_no_FD = ['brent_0', 'brent_dyn']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # CSPI ==============================================
    # GATHER CSPI
    name = 'CSPI_0'
    list_no_FD = ['cspi_0', 'cspiQ']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER CSPI LW
    name = 'CSPI_DYN_VOL_ST'
    list_no_FD =  ['cspi_LD', 'cspi_LW', 'cspi_VOL_W', 'cspi_LD_S', 'cspi_LW_S', 'cspi_VOL_W_S',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER CSPI LM
    name = 'CSPI_DYN_VOL_LT'
    list_no_FD =  ['cspi_LM', 'cspi_VOL_M', 'cspi_LM_S', 'cspi_VOL_M_S', 'cspi_LY', 'cspi_LY_S',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # FINANCE ==========================================
    name = 'POWER_FY_DYN_LT'
    list_no_FD = ['POWER_FY_DYN_LY', 'POWER_FY_DYN_LY_nst', 'POWER_FY_DYN_LM', 'POWER_FY_MA',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    name = 'POWER_FY_DYN_ST'
    list_no_FD = ['POWER_FY_DYN_LW',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FM_DYN_LT'
    list_no_FD = ['POWER_FM_DYN_LY', 'POWER_FM_DYN_LY_nst', 'POWER_FM_DYN_LM', 'POWER_FM_MA',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FM_DYN_ST'
    list_no_FD = ['POWER_FM_DYN_LW',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FY'
    list_no_FD = ['POWER_FY', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FQ'
    list_no_FD = ['POWER_FQ', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FM'
    list_no_FD = ['POWER_FM', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            if el != name:
                map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_OHLC'
    list_no_FD = ['s', 'ohlc', 'ohlcL']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_RSI'
    list_no_FD = ['rsi', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_SLOWSTOCH'
    list_no_FD = ['slowstoch', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_MACD'
    list_no_FD = ['macd']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_MACD_LIN'
    list_no_FD = ['macd_lin']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_FWC'
    list_no_FD = ['fwc', 'fwc_dyn',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER
    name = 'POWER_DYN_VOL'
    list_no_FD = ['maperc_LT', 'maperc_ST', 'maperclh_ST', 'slt_dyn', 'vol', 'vol_p',
                  'rank_ST', 'rank_LT', 'sintra', 'sca', 'rshocks', 'rshocks_adj', 'reshocks', 'reshocks_adj' ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER GAS_STORAGE
    name = 'GAS_STORAGE_0'
    list_no_FD = ['gas_sto_i']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER GAS_STORAGE
    name = 'GAS_STORAGE_ST'
    list_no_FD = ['gas_sto_hst']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER GAS_STORAGE
    name = 'GAS_STORAGE_LT'
    list_no_FD = ['gas_sto_hlt', 'gas_sto_vlt', 'gas_sto_hlt_sub', 'gas_sto_hlt_P']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER LNG_STORAGE
    name = 'LNG_STORAGE_0'
    list_no_FD =  ['lngstorei', 'lngstorei_so']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER LNG_STORAGE
    name = 'LNG_STORAGE_ST'
    list_no_FD = ['lngstorehst']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER LNG_STORAGE
    name = 'LNG_STORAGE_LT'
    list_no_FD = ['lngstorehlt', 'lngstorehvlt', 'lngstorehlt_so']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER LNG FLOW
    name = 'LNG_FLOW_0'
    list_no_FD = ['lngflow_D', 'lngflow_D1_nst',
                  'lngflow_W_EU', 'lngflow_W_EEU', 'lngflow_W_EAS', 'lngflow_W_SEAS',  'lngflow_CONG',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'LNG_FLOW_ST'
    list_no_FD = ['lngflow_ST_EU', 'lngflow_ST_EEU', 'lngflow_ST_EAS', 'lngflow_ST_SEAS',
                  'lngflow_LT_EU', 'lngflow_LT_EEU', 'lngflow_LT_EAS', 'lngflow_LT_SEAS', 'lngflow_LM',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'LNG_FLOW_SPEC'
    list_no_FD = ['lngflow_SPEC',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'LNG_FLOW_LT'
    list_no_FD = ['lngflow_LY', 'lngflow_LY_EU',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'LNG_FLOW_CNN'
    list_no_FD = ['lngflow_CNN_L0_FST', 'lngflow_CNN_L0_FLT', 'lngflow_CNN_LST_FST', 'lngflow_CNN_LST_FLT',
                  'lngflow_CNN_LLT_FST', 'lngflow_CNN_LLT_FLT', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER COALFLOW
    name = 'COALFLOW_0'
    list_no_FD = ['coalflow_D', 'coalflow_W',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER COALFLOW
    name = 'COALFLOW_ST'
    list_no_FD = ['coalflow_LW', 'coalflow_LM',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER COALFLOW
    name = 'COALFLOW_LT'
    list_no_FD = ['coalflow_LY',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER CRUDEFLOW
    name = 'CRUDEFLOW_0'
    list_no_FD = ['crudeflow_D', 'crudeflow_W_EU', 'crudeflow_W_EEU', 'crudeflow_W_EAS', 'crudeflow_W_SEAS',
                  'crudeflow_W_NE_AM', 'crudeflow_W_NW_AM', ]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'CRUDEFLOW_ST'
    list_no_FD = ['crudeflow_LW', 'crudeflow_LM',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'CRUDEFLOW_LT'
    list_no_FD = ['crudeflow_LY',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER CRUDE STORAGE
    name = 'CRUDE_OFFSHORE_STORAGE_0'
    list_no_FD = ['crude_offstorage_0', 'crude_offstorage_AG_0']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'CRUDE_OFFSHORE_STORAGE_ST'
    list_no_FD = ['crude_offstorage_LW', 'crude_offstorage_LM', 'crude_offstorage_AG_LW', 'crude_offstorage_AG_LM']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'CRUDE_OFFSHORE_STORAGE_LT'
    list_no_FD = ['crude_offstorage_LY', 'crude_offstorage_AG_LY']
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_OTHERS'
    list_no_FD = ['snow', 'sW', 'sW_z', 'season',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_TEMP_0'
    list_no_FD = ['weather0_temp',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_CLOUD_0'
    list_no_FD = ['weather0_cloud',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_WIND_0'
    list_no_FD = ['weather0_wind',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_TEMP_ST'
    list_no_FD = ['weather_FM_MA_temp',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    name = 'WEATHER_TEMP_LT'
    list_no_FD = ['weather_FM_LY_temp_de', 'weather_FM_LY_temp_fr', 'weather_FM_LY_temp_it', 'weather_FM_LY_temp_po',
                  'weather_FM_LY_temp_jp', 'weather_FM_LY_temp_us', 'weather_FM_LY_temp_au', 'weather_FM_LY_temp_nb',
                  'weather_FM_LY_temp_ch', 'weather_FM_LY_temp_es', 'weather_FM_LY_temp_ce', 'weather_FM_LY_temp_med',
                  'weather_FM_LY_temp_ee',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_CLOUD_ST'
    list_no_FD = ['weather_FM_MA_cloud',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_CLOUD_LT'
    list_no_FD = ['weather_FM_LY_cloud',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_WIND_ST'
    list_no_FD = ['weather_FM_MA_wind',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER WEATHER
    name = 'WEATHER_WIND_LT'
    list_no_FD = ['weather_FM_LY_wind',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER PRODUCT
    name = 'OTHERS'
    list_no_FD = ['sizematu', 'country', 'base',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # GATHER CORR FUND
    name = 'FUND_CORRELATION'
    list_no_FD = ['corr_fund_ST', 'corr_fund_MT', 'corr_fund_LT',
                  'corr_fund_ST_LW', 'corr_fund_MT_LW', 'corr_fund_LT_LW',]
    list_FD = [el + FRACDIFF for el in list_no_FD]
    for el in list_FD + list_no_FD:
        if el in list_group:
            if name in list(map_group_lvl2.keys()):
                map_group_lvl2[name] = map_group_lvl2[name] + map_group_lvl2[el]
            else:
                map_group_lvl2[name] = map_group_lvl2[el]
            map_group_lvl2.pop(el)
    # =======================================================
    # MAP LEVEL 1 ===========================================
    map_group_lvl1 = copy.deepcopy(map_group_lvl2)
    list_group = list(map_group_lvl1.keys())
    name = 'UNDERLYINGS'
    for el in ['DA', 'IMBALANCE', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'CO2'
    for el in ['EUA_0', 'EUA_DYN', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'COAL'
    for el in ['API2_DARK_CLEAN', 'API2_DARK_CLEAN_DYN_VOL_ST', 'API2_DARK_CLEAN_DYN_VOL_LT', 'COAL_FLOW', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'GAS'
    for el in ['TTF_SPARK_CLEAN', 'TTF_SPARK_CLEAN_DYN_VOL_ST', 'TTF_SPARK_CLEAN_DYN_VOL_LT',
               'GAS_NO_TTF', 'GAS_TTFJKM',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'GAS_STORAGE'
    for el in ['GAS_STORAGE_0', 'GAS_STORAGE_ST', 'GAS_STORAGE_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'LNG_STORAGE'
    for el in ['LNG_STORAGE_0', 'LNG_STORAGE_ST', 'LNG_STORAGE_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'LNG_FLOW'
    for el in ['LNG_FLOW_0', 'LNG_FLOW_CONG', 'LNG_FLOW_ST', 'LNG_FLOW_SPEC', 'LNG_FLOW_LT']:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'LNG_FLOW_CNN'
    for el in ['LNG_FLOW_CNN', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'COAL_FLOW'
    for el in ['COALFLOW_0', 'COALFLOW_ST', 'COALFLOW_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'CRUDE_FLOW'
    for el in ['CRUDEFLOW_0', 'CRUDEFLOW_ST', 'CRUDEFLOW_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'CRUDE_STORAGE'
    for el in ['CRUDE_OFFSHORE_STORAGE_0', 'CRUDE_OFFSHORE_STORAGE_ST', 'CRUDE_OFFSHORE_STORAGE_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'CSPI'
    for el in ['CSPI_0', 'CSPI_DYN_VOL_ST', 'CSPI_DYN_VOL_LT', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'POWER'
    for el in ['POWER_OHLC', 'POWER_FWC', 'POWER_FY', 'POWER_FQ', 'POWER_FY_DYN_ST', 'POWER_FY_DYN_LT',
               'POWER_FM', 'POWER_FM_DYN_ST', 'POWER_FM_DYN_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'FINANCIAL'
    for el in ['POWER_RSI', 'POWER_SLOWSTOCH', 'POWER_MACD', 'POWER_MACD_LIN',
               'POWER_DYN_VOL', ]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'WEATHER_TEMP'
    for el in ['WEATHER_TEMP_0', 'WEATHER_TEMP_ST', 'WEATHER_TEMP_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'WEATHER_CLOUD'
    for el in ['WEATHER_CLOUD_0', 'WEATHER_CLOUD_ST', 'WEATHER_CLOUD_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)
    name = 'WEATHER_WIND'
    for el in ['WEATHER_WIND_0', 'WEATHER_WIND_ST', 'WEATHER_WIND_LT',]:
        if el in list_group:
            if name in list(map_group_lvl1.keys()):
                map_group_lvl1[name] = map_group_lvl1[name] + map_group_lvl1[el]
            else:
                map_group_lvl1[name] = map_group_lvl1[el]
            map_group_lvl1.pop(el)

    # RM NULLKEY() & CHECK MECE FOR ALL LEVELS
    map_group_lvl2 = sanity_check(map_group_lvl2)
    map_group_lvl1 = sanity_check(map_group_lvl1)
    bool_MECE2, list_is_not_included2, dict_intersection2 = verif_MECE(map_group_lvl2, lfall)
    bool_MECE1, list_is_not_included1, dict_intersection1 = verif_MECE(map_group_lvl1, lfall)
    if (not bool_MECE2) | (not bool_MECE1):
        if list_is_not_included2:
            print(f'MISSING FEATURE IN MAPPING TO SUBGROUP_lvl2: {list_is_not_included2}')
            print('FIX : ADD FEAT AS ITS OWN GROUP')
            for el in list_is_not_included2:
                map_group_lvl2[el] = [el]
        if dict_intersection2:
            print(f'FEATURE OVERLAP IN MAPPING TO SUBGROUP_lvl2: {dict_intersection2}')
            print(f'FIX : KEEP ONLY FIRST GROUP')
            for feat_ in list(dict_intersection2.keys()):
                for group_ in dict_intersection2[feat_][1:]:
                    map_group_lvl2[group_].remove(feat_)
                    print(f'RM {feat_} FROM {group_}')
        if list_is_not_included1:
            print(f'MISSING FEATURE IN MAPPING TO SUBGROUP_lvl1: {list_is_not_included1}')
            print('FIX : ADD FEAT AS ITS OWN GROUP')
            for el in list_is_not_included1:
                map_group_lvl1[el] = [el]
        if dict_intersection1:
            print(f'FEATURE OVERLAP IN MAPPING TO SUBGROUP_lvl1: {dict_intersection1}')
            print(f'FIX : KEEP ONLY FIRST GROUP')
            for feat_ in list(dict_intersection1.keys()):
                for group_ in dict_intersection1[feat_][1:]:
                    map_group_lvl1[group_].remove(feat_)
                    print(f'RM {feat_} FROM {group_}')

    return map_group_lvl3, map_group_lvl2, map_group_lvl1

