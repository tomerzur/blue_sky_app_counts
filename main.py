import pandas as pd
import redshift_connector
import streamlit as st
from st_aggrid import AgGrid


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    redshift_username = st.secrets['redshift_username']
    redshift_password = st.secrets['redshift_password']
    conn = redshift_connector.connect(
        host='cue-development-cluster.cvwbvpbngpcb.us-east-1.redshift.amazonaws.com',
        database='cue_development',
        port=5432,
        user=redshift_username,
        password=redshift_password
    )
    # st.write("Enter a brand name: ")
    #brand_name = 'ancestry'
    brand_name = st.text_input("Enter a brand name: ", 'ancestry')

    query = f'''
with website_app_names as (
select brand_name, min(publisher) as publisher,
  listagg(distinct website_name,', ') AS website_names, listagg(distinct app_name,', ') as app_names 
  from (
        select wp.start_time_local, wp.taxonomy_id, wp.device_id, wp.month_key, wp.panelist_id, wp.duration, 'website' as accessed_with
        from supplementary.luth_web_page_view_events wp
        UNION
        select app.start_time_local, app.taxonomy_id, app.device_id, app.month_key, app.panelist_id, app.duration, 'app' as accessed_with
        from supplementary.luth_app_foreground_sessions app
      ) base
      join supplementary.luth_taxonomy_reference t on base.taxonomy_id = t.taxonomy_id
where brand_name ilike '{brand_name}%'
group by brand_name
order by brand_name
),

panelist_cts as (
select brand_name,
  count(distinct panelist_id) as ct
  from (
        select wp.start_time_local, wp.taxonomy_id, wp.device_id, wp.month_key, wp.panelist_id, wp.duration, 'website' as accessed_with
        from supplementary.luth_web_page_view_events wp
        UNION
        select app.start_time_local, app.taxonomy_id, app.device_id, app.month_key, app.panelist_id, app.duration, 'app' as accessed_with
        from supplementary.luth_app_foreground_sessions app
      ) base
      join supplementary.luth_taxonomy_reference t on base.taxonomy_id = t.taxonomy_id
where brand_name ilike '{brand_name}%'
group by brand_name
order by brand_name
)

select n.brand_name, n.publisher, c.ct, n.website_names, n.app_names
from website_app_names n join panelist_cts c on n.brand_name = c.brand_name
'''

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetch_dataframe()
    result = result.sort_values(by=['ct'], ascending=False)
    AgGrid(result)
    print()
