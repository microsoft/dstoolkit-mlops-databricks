
@patch("builtins.open")
def test_ingest_json_parameters_file(mock_open):
    mock_open.return_value = MagicMock(__enter__=lambda _: json.dumps({"Clusters": [{"name": "test_cluster"}]}))
    json_cluster_param_file = ingest_json_parameters_file("test_environment")
    assert json_cluster_param_file == [{"name": "test_cluster"}]

class TestListClusters(unittest.TestCase):
    @patch('python.utils_create_clusters._list_clusters')
    def test_list_clusters_successful(self, mock_list_clusters):
        mock_list_clusters.return_value = ({'clusters': [{'cluster_name': 'test_cluster'}]}, 0)
        existing_clusters, return_code = _list_clusters()

        self.assertEqual(return_code, 0)
        self.assertEqual(existing_clusters, {'clusters': [{'cluster_name': 'test_cluster'}]})


def test_list_clusters_failure():
    with patch('main.requests.get') as mock_get:
        mock_get.return_value.status_code = 500
        with pytest.raises(Exception):
            _list_clusters()

def test_create_cluster_successful():
    with patch('main.requests.post') as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'cluster_id': '12345'}
        result = _create_cluster({'cluster_name': 'test_cluster'})
        assert result == (200, '12345')

def test_create_cluster_failure():
    with patch('main.requests.post') as mock_post:
        mock_post.return_value.status_code = 500
        with pytest.raises(Exception):
            _create_cluster({'cluster_name': 'test_cluster'})

def test_get_dbrks_cluster_info_successful():
    with patch('main.requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b'{"state": "RUNNING"}'
        result = _get_dbrks_cluster_info('12345')
        assert result == {'state': 'RUNNING'}

def test_get_dbrks_cluster_info_failure():
    with patch('main.requests.get') as mock_get:
        mock_get.return_value.status_code = 500
        with pytest.raises(Exception):
            _get_dbrks_cluster_info('12345')

def test_list_existing_clusters():
    with patch.dict('os.environ', 
                    {'WORKSPACE_ID': 'test_workspace_id',
                    'DATABRICKS_INSTANCE': 'test_instance',
                    'DATABRICKS_AAD_TOKEN': 'test_aad_token',
                    'DATABRICKS_MANAGEMENT_TOKEN': 'test_management_token', 
                    'environment': 'test_environment'
                    }):
        
        with patch('main._list_clusters') as mock_list_clusters:
            mock_list_clusters.return_value = {'clusters': [{'cluster_name': 'test_cluster1'}, {'cluster_name': 'test_cluster2'}]}
            result = list_existing_clusters()
            assert result == ['test_cluster1', 'test_cluster2']

