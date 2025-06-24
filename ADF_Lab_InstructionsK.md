# Lab ADF. Azure Data Factory를 사용한 영화 분석

*Azure Data Factory를 처음 사용하는 경우, [Azure Data Factory 소개](https://docs.microsoft.com/azure/data-factory/introduction)를 참조하십시오.*

이 실습에서는 Azure Data Factory의 시각적 작성 환경을 활용하여 GitHub 리포지토리에 저장된 영화 데이터를 Azure Data Lake Storage Gen2로 복사한 다음, Mapping Data Flow를 실행하여 데이터를 변환하고 Azure SQL Database에 쓰는 Pipeline을 만듭니다.

이 실습에서 사용되는 패턴은 Azure Data Factory를 사용한 최신 데이터 웨어하우스 수집 및 변환 시나리오의 예입니다.

이 실습에서 만든 Pipeline은 Azure Data Factory [템플릿 갤러리](https://azure.microsoft.com/blog/get-started-quickly-using-templates-in-azure-data-factory/)에서 **Movie Analytics**라는 이름으로 제공됩니다.

이 실습을 처음부터 끝까지 완료하는 데 약 2시간이 소요됩니다.

## 사전 준비물

*   **Azure 구독**: Azure 구독이 없는 경우 시작하기 전에 [체험 계정](https://azure.microsoft.com/free/)을 만드십시오.

*   **Azure Data Lake Storage Gen2 스토리지 계정**: ADLS Gen2 스토리지 계정이 없는 경우 [ADLS Gen2 스토리지 계정 만들기](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account)의 지침을 참조하십시오.

*   **Azure SQL Database 계정**: SQL DB 계정이 없는 경우 [SQL DB 계정 만들기](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-single-database-get-started?tabs=azure-portal)의 지침을 참조하십시오.

## 환경 설정

**Data Factory 생성:** [Azure Portal](https://portal.azure.com)을 사용하여 Data Factory를 만듭니다. 자세한 지침은 [Data Factory 만들기](https://docs.microsoft.com/azure/data-factory/quickstart-create-data-factory-portal)에서 찾을 수 있습니다.

1.  Azure Portal에 접속한 후, 왼쪽 메뉴에서 **All Services** 버튼을 클릭하고 Analytics 섹션에서 "Data Factories"를 선택합니다.
    ![Azure Portal](./assets/MovieAnalytics/portal1.png)

2.  **Add**를 클릭하여 Data Factory 만들기 화면을 엽니다.
    ![Azure Portal](./assets/MovieAnalytics/portal2.png)

3.  만들기 창에서 Data Factory 구성 설정을 지정합니다. 전역적으로 고유한 Data Factory 이름을 선택하고 구독, 리소스 그룹 및 지역을 선택합니다. Data Factory 버전은 V2여야 합니다. 완료되면 **Create**를 클릭합니다. Data Factory 배포에는 몇 분 정도 걸릴 수 있습니다.
    *   Mapping Data Flow는 현재 West Central US, Brazil South, Korea Central, France Central 지역에서는 사용할 수 없습니다. 이 실습에서는 이러한 지역에 Data Factory를 만들지 마십시오.
    *   Azure DevOps 및 GitHub와의 ADF 통합은 이 실습에서 다루지 않습니다. 이 기능을 사용하려면 **Enable Git**을 선택하고 구성 정보를 지정하십시오. [Azure Data Factory의 소스 제어](https://docs.microsoft.com/azure/data-factory/source-control#troubleshooting-git-integration)를 참조하십시오.
    ![Azure Portal](./assets/MovieAnalytics/portal3.png)

4.  Data Factory가 배포되면 리소스로 이동하여 **Authoring and Monitoring**을 클릭하여 ADF 사용자 환경(UX)을 엽니다. ADF UX는 adf.azure.com을 통해 액세스할 수 있습니다.
    ![Azure Portal](./assets/MovieAnalytics/portal4.png)

## Azure Data Lake Storage Gen2로 데이터 수집

Data Factory가 생성되고 ADF UX를 열면, Pipeline의 첫 번째 단계는 S3 또는 GitHub에서 ADLS Gen2 스토리지로 moviesDB.csv 파일을 복사하는 Copy Activity를 만드는 것입니다.

1.  **작성 캔버스 열기**: ADF 홈페이지에서 시작하는 경우 왼쪽 사이드바의 연필 아이콘을 클릭하거나 파이프라인 만들기 버튼을 클릭하여 작성 캔버스를 엽니다.
    ![Authoring](./assets/MovieAnalytics/authoring1.png)
2.  **Pipeline 만들기**: Factory Resources 창에서 + 버튼을 클릭하고 Pipeline을 선택합니다.
    ![Authoring](./assets/MovieAnalytics/authoring2.png)
3.  **Copy Activity 추가**: Activities 창에서 Move and Transform 아코디언을 열고 Copy Data Activity를 Pipeline 캔버스로 드래그합니다.
    ![Authoring](./assets/MovieAnalytics/authoring3.png)
    Copy Activity의 Source 탭에서 'Preview Data'를 클릭하여 데이터의 작은 스냅샷을 가져옵니다.
    ![Authoring](./assets/MovieAnalytics/authoring13.png)
4.  **Source로 사용할 새 HTTP Dataset 만들기**
    1.  Copy Activity 설정의 Source 탭에서 '+ New'를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring4.png)
    2.  데이터 저장소 목록에서 HTTP 타일을 선택하고 Continue를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring10.png)
    3.  파일 형식 목록에서 DelimitedText 형식 타일을 선택하고 Continue를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring6.png)
    4.  Set Properties 사이드 탐색 창에서 Dataset에 이해하기 쉬운 이름을 지정하고 Linked Service 드롭다운을 클릭합니다. HTTP Linked Service를 만들지 않은 경우 'New'를 선택합니다.
    ![Authoring](./assets/MovieAnalytics/authoring7.png)
    5.  HTTP Linked Service 구성 창에서 moviesDB csv 파일의 URL을 지정합니다. 다음 엔드포인트를 사용하여 인증 없이 데이터에 액세스할 수 있습니다.

    `https://raw.githubusercontent.com/djpmsft/adf-ready-demo/master/moviesDB.csv`

    ![Authoring](./assets/MovieAnalytics/authoring11.png)
    a. Linked Service를 만들고 선택한 후 나머지 Dataset 설정을 지정합니다. 이 설정은 연결에서 데이터를 가져올 방법과 위치를 지정합니다. URL이 이미 파일을 가리키고 있으므로 상대 엔드포인트는 필요하지 않습니다. 데이터의 첫 번째 행에 헤더가 있으므로 'First row as header'를 true로 설정하고 'Import schema from connection/store'를 선택하여 파일 자체에서 스키마를 가져옵니다. 요청 메서드로 Get을 선택합니다. 완료되면 'Finish'를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring12.png)
    a. Dataset이 올바르게 구성되었는지 확인하려면 Copy Activity의 Source 탭에서 'Preview Data'를 클릭하여 데이터의 작은 스냅샷을 가져옵니다.
    ![Authoring](./assets/MovieAnalytics/authoring13.png)
5.  **새 ADLS Gen2 Dataset Sink 만들기**
    1.  Sink 탭에서 + New를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring14.png)
    2.  Azure Data lake Storage Gen2 타일을 선택하고 Continue를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring15.png)
    3.  DelimitedText 형식 타일을 선택하고 Continue를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring6.png)
    4.  Set Properties 사이드 탐색 창에서 Dataset에 이해하기 쉬운 이름을 지정하고 Linked Service 드롭다운을 클릭합니다. ADLS Linked Service를 만들지 않은 경우 'New'를 선택합니다.
    ![Authoring](./assets/MovieAnalytics/authoring7.png)
    5.  ADLS Linked Service 구성 창에서 인증 방법을 선택하고 자격 증명을 입력합니다. 아래 예에서는 계정 키를 사용하고 드롭다운에서 스토리지 계정을 선택했습니다.
    ![Authoring](./assets/MovieAnalytics/authoring16.png)
    6.  Linked Service를 구성한 후 ADLS Dataset 구성을 입력합니다. 이 Dataset에 쓰는 것이므로 moviesDB.csv를 복사할 폴더와 파일을 지정해야 합니다. 아래 예에서는 'sample-data' 컨테이너의 'output' 폴더에 'moviesDB.csv' 파일로 쓰고 있습니다. 폴더와 파일은 동적으로 생성될 수 있지만 컨테이너는 쓰기 전에 존재해야 합니다. 'First row as header'를 true로 설정합니다. 이 설정을 지정하지 않으면 데이터가 헤더 없이 작성되어 나중에 실습에서 문제가 발생할 수 있습니다. 이때 스키마를 지정하지 마십시오. 완료되면 Finish를 클릭합니다.
    ![Authoring](./assets/MovieAnalytics/authoring17.png)

이 시점에서 Copy Activity 구성이 완료되었습니다. 테스트하려면 Pipeline 캔버스 상단의 Debug 버튼을 클릭하십시오. 이렇게 하면 Pipeline 디버그 실행이 시작됩니다.

![Authoring](./assets/MovieAnalytics/authoring18.png)

Pipeline 디버그 실행 진행 상황을 모니터링하려면 Pipeline의 Output 탭을 클릭하십시오.

![Copy output](./assets/MovieAnalytics/CopyOutput.PNG "Copy output")

Activity 출력에 대한 자세한 설명을 보려면 안경 모양 아이콘을 클릭하십시오. 이렇게 하면 데이터 읽기/쓰기, 처리량, 심층적인 기간 통계와 같은 유용한 메트릭을 제공하는 복사 모니터링 화면이 열립니다.

![Copy monitoring](./assets/MovieAnalytics/CopyMonitoring.PNG "Copy monitoring")

복사가 예상대로 작동했는지 확인하려면 ADLS Gen2 스토리지 계정을 열고 파일이 예상대로 작성되었는지 확인하십시오.

## Mapping Data Flow로 데이터 변환

이제 데이터를 ADLS로 옮겼으므로 Spark 클러스터를 통해 대규모로 데이터를 변환한 다음 Data Warehouse에 로드하는 Mapping Data Flow를 만들 준비가 되었습니다. Mapping Data Flow에 대한 자세한 내용은 [Mapping Data Flow 설명서](https://docs.microsoft.com/azure/data-factory/concepts-data-flow-overview)를 참조하십시오.

1.  **Data Flow Debug 켜기**: 작성 모듈 상단에 있는 Data Flow Debug 슬라이더를 켭니다. Data Flow 클러스터는 예열하는 데 5-7분이 걸리므로 Data Flow 개발을 계획하고 있다면 먼저 디버그를 켜는 것이 좋습니다. 자세한 내용은 [디버그 모드](https://docs.microsoft.com/azure/data-factory/concepts-data-flow-debug-mode)를 참조하십시오.
    ![Data Flow](./assets/MovieAnalytics/dataflow1.png)
2.  **Data Flow Activity 추가**: Activities 창에서 Move and Transform 아코디언을 열고 Data Flow Activity를 Pipeline 캔버스로 드래그합니다. 나타나는 사이드 탐색 창에서 Create new Data Flow를 선택하고 Mapping Data Flow를 선택합니다. Pipeline 캔버스로 돌아가서 Copy Activity의 녹색 상자를 Data Flow Activity로 드래그하여 성공 시 실행 조건을 만듭니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow2.png)
3.  **ADLS Source 추가**: Data Flow 캔버스를 엽니다. Data Flow 캔버스에서 Add Source 버튼을 클릭합니다. Source Dataset 드롭다운에서 Copy Activity에 사용된 ADLS Gen2 Dataset을 선택합니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow3.png)
    *   Dataset이 다른 파일이 있는 폴더를 가리키는 경우, 다른 Dataset을 만들거나 매개 변수화를 활용하여 moviesDB.csv 파일만 읽도록 해야 할 수 있습니다.
    *   ADLS에서 스키마를 가져오지 않았지만 이미 데이터를 수집한 경우, Dataset의 'Schema' 탭으로 이동하여 'Import schema'를 클릭하여 Data Flow가 스키마 프로젝션을 알 수 있도록 합니다.

    디버그 클러스터가 예열되면 Data Preview 탭을 통해 데이터가 올바르게 로드되었는지 확인합니다. Refresh 버튼을 클릭하면 Mapping Data Flow가 각 변환 단계에서 데이터가 어떻게 보이는지에 대한 스냅샷을 계산하여 보여줍니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow4.png)

4.  **Select 변환을 추가하여 열 이름 변경 및 삭제**: Rotton Tomatoes 열 이름에 오타가 있는 것을 발견했을 수 있습니다. 이름을 올바르게 지정하고 사용하지 않는 Rating 열을 삭제하려면 ADLS Source 노드 옆의 + 아이콘을 클릭하고 Schema modifier 아래에서 Select를 선택하여 [Select 변환](https://docs.microsoft.com/azure/data-factory/data-flow-select)을 추가합니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow5.png)

    Name as 필드에서 'Rotton'을 'Rotten'으로 변경합니다. Rating 열을 삭제하려면 해당 열 위로 마우스를 가져가 휴지통 아이콘을 클릭합니다.

    ![Select](./assets/MovieAnalytics/Select.PNG "Select")

5.  **Filter 변환을 추가하여 원치 않는 연도 필터링**: 1951년 이후에 제작된 영화에만 관심이 있다고 가정해 봅시다. Select 변환 옆의 + 아이콘을 클릭하고 Row Modifier 아래에서 Filter를 선택하여 [Filter 변환](https://docs.microsoft.com/azure/data-factory/data-flow-filter)을 추가하여 필터 조건을 지정할 수 있습니다. 표현식 상자를 클릭하여 [표현식 빌더](https://docs.microsoft.com/azure/data-factory/concepts-data-flow-expression-builder)를 열고 필터 조건을 입력합니다. [Mapping Data Flow 표현식 언어](https://docs.microsoft.com/azure/data-factory/data-flow-expression-functions)의 구문을 사용하여, **`toInteger(year) > 1950`**은 문자열인 `year` 값을 정수로 변환하고 해당 값이 1950보다 큰 경우 행을 필터링합니다.
    *   `toInteger(inputValue)`: 입력값을 정수형으로 변환합니다.
    *   `year`: 영화의 제작 연도 열을 나타냅니다.

    ![Filter](./assets/MovieAnalytics/Filter.PNG "Filter")

    표현식 빌더의 내장된 Data Preview 창을 사용하여 조건이 올바르게 작동하는지 확인할 수 있습니다.

    ![Filter Expression](./assets/MovieAnalytics/FilterExpression.PNG "Filter Expression")

6.  **Derive 변환을 추가하여 주 장르 계산**: genres 열이 '|' 문자로 구분된 문자열인 것을 알 수 있습니다. 각 열의 첫 번째 장르에만 관심이 있다면 Filter 변환 옆의 + 아이콘을 클릭하고 Schema Modifier 아래에서 Derived를 선택하여 [Derived Column 변환](https://docs.microsoft.com/azure/data-factory/data-flow-derived-column)을 통해 새 열을 파생할 수 있습니다. Filter 변환과 유사하게, Derived Column은 Mapping Data Flow 표현식 빌더를 사용하여 새 열의 값을 지정합니다.

    ![Derive](./assets/MovieAnalytics/Derive.PNG "Derive")

    이 시나리오에서는 'genre1|genre2|...|genreN' 형식인 genres 열에서 첫 번째 장르를 추출하려고 합니다. **`locate`** 함수를 사용하여 genres 문자열에서 '|'의 첫 번째 (1부터 시작하는) 인덱스를 가져옵니다. **`iif`** 함수를 사용하여 이 인덱스가 1보다 크면, **`left`** 함수를 통해 주 장르를 계산할 수 있습니다 (이 함수는 문자열에서 특정 인덱스 왼쪽의 모든 문자를 반환합니다). 그렇지 않으면 PrimaryGenre 값은 genres 필드와 같습니다. 표현식 빌더의 Data Preview 창을 통해 출력을 확인할 수 있습니다.
    *   `locate(searchString, stringToSearch, [startIndex])`: `stringToSearch` 내에서 `searchString`이 처음 나타나는 위치(1부터 시작)를 반환합니다.
    *   `iif(condition, trueValue, falseValue)`: `condition`이 참이면 `trueValue`를, 거짓이면 `falseValue`를 반환합니다.
    *   `left(string, numberOfChars)`: 문자열의 왼쪽에서 지정된 `numberOfChars` 만큼의 문자를 반환합니다. 여기서는 `left(genres, locate('|', genres) - 1)` 와 같이 사용될 것입니다.

    ![Derive output](./assets/MovieAnalytics/DeriveOutput.PNG "Derive output")

7.  **Window 변환을 통해 영화 순위 매기기**: 특정 장르 내 해당 연도 영화의 순위에 관심이 있다고 가정해 봅시다. Derived Column 변환 옆의 + 아이콘을 클릭하고 Schema modifier 아래에서 Window를 클릭하여 [Window 변환](https://docs.microsoft.com/azure/data-factory/data-flow-window)을 추가하여 창 기반 집계를 정의할 수 있습니다. 이를 위해 무엇을 기준으로 창을 나눌지(Over), 무엇으로 정렬할지(Sort), 범위는 어떻게 할지(Range), 새 창 열을 어떻게 계산할지(Window columns) 지정합니다. 이 예에서는 PrimaryGenre와 year를 기준으로 창을 나누고, 범위는 제한 없이(unbounded) 설정하며, Rotten Tomato 점수를 기준으로 내림차순 정렬하고, 각 영화가 해당 장르-연도 내에서 갖는 순위와 동일한 RatingsRank라는 새 열을 계산합니다.

    ![Window Over](./assets/MovieAnalytics/WindowOver.PNG "Window Over")

    ![Window Sort](./assets/MovieAnalytics/WindowSort.PNG "Window Sort")

    ![Window Bound](./assets/MovieAnalytics/WindowBound.PNG "Window Bound")

    ![Window Rank](./assets/MovieAnalytics/WindowRank.PNG "Window Rank")

8.  **Aggregate 변환으로 평점 집계**: 필요한 모든 데이터를 수집하고 파생했으므로, Window 변환 옆의 + 아이콘을 클릭하고 Schema modifier 아래에서 Aggregate를 클릭하여 [Aggregate 변환](https://docs.microsoft.com/azure/data-factory/data-flow-aggregate)을 추가하여 원하는 그룹을 기준으로 메트릭을 계산할 수 있습니다. Window 변환에서 했던 것처럼 장르와 연도별로 영화를 그룹화합니다.

    ![Agg group by](./assets/MovieAnalytics/AggGroupBy.PNG "Agg group by")

    Aggregates 탭에서 지정된 group by 열에 대해 계산된 집계를 설정할 수 있습니다. 모든 장르와 연도에 대해 평균 Rotten Tomatoes 평점, 가장 높고 낮은 평점의 영화(Windowing 함수 활용), 각 그룹에 속한 영화 수를 가져옵니다. 집계는 변환 스트림의 행 수를 크게 줄이고 변환에 지정된 group by 및 집계 열만 전파합니다.

    *   집계 변환이 데이터를 어떻게 변경하는지 보려면 Data Preview 탭을 사용하십시오.

    ![Aggregate](./assets/MovieAnalytics/Aggregate.PNG "Aggregate")

9.  **Alter Row 변환을 통해 Upsert 조건 지정**: 테이블 형식 Sink에 쓰는 경우, Aggregate 변환 옆의 + 아이콘을 클릭하고 Row modifier 아래에서 Alter Row를 클릭하여 [Alter Row 변환](https://docs.microsoft.com/azure/data-factory/data-flow-alter-row)을 사용하여 행에 대한 삽입, 삭제, 업데이트 및 업서트 정책을 지정할 수 있습니다. 항상 삽입 및 업데이트하므로 모든 행이 항상 업서트되도록 지정할 수 있습니다. (Upsert: Update or Insert의 줄임말로, 데이터가 존재하면 업데이트하고 존재하지 않으면 삽입하는 동작)

    ![Upsert](./assets/MovieAnalytics/AlterRow.PNG "Upsert")

10. **SQL DB Sink에 쓰기**: 모든 변환 로직을 완료했으므로 이제 Sink에 쓸 준비가 되었습니다.
    1.  Upsert 변환 옆의 + 아이콘을 클릭하고 Destination 아래에서 Sink를 클릭하여 Sink를 추가합니다.
    2.  Sink 탭에서 + New 버튼을 통해 새 SQL DB Dataset을 만듭니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow6.png)
    3.  타일 목록에서 Azure SQL Data Database를 선택합니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow7.png)
    4.  새 Linked Service를 선택하고 SQL DB 연결 자격 증명을 구성합니다. 완료되면 'Create'를 클릭합니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow8.png)
    5.  Dataset 구성에서 'Create new table'을 선택하고 원하는 테이블 이름을 입력합니다. 완료되면 'Finish'를 클릭합니다.
    ![Data Flow](./assets/MovieAnalytics/dataflow9.png)
    6.  Upsert 조건이 지정되었으므로 Settings 탭으로 이동하여 키 열 PrimaryGenre와 year를 기준으로 'Allow upsert'를 선택해야 합니다.
    ![Sink](./assets/MovieAnalytics/Sink.PNG "Sink")

이 시점에서 8개의 변환으로 구성된 Mapping Data Flow 구축을 완료했습니다. 이제 Pipeline을 실행하고 결과를 확인할 차례입니다!

![Data Flow Canvas](./assets/MovieAnalytics/DataFlowCanvas.PNG "Data Flow Canvas")

## Pipeline 실행

Pipeline 캔버스로 이동합니다. Execute Data Flow Activity의 Settings 탭에서 선택한 Data Flow와 사용된 컴퓨팅 환경을 볼 수 있습니다. 이 실습에서는 기본 4코어 범용 클러스터를 사용해야 합니다.

![Data Flow](./assets/MovieAnalytics/pipeline1.png)

Pipeline을 게시하기 전에 다른 디버그 실행을 통해 예상대로 작동하는지 확인하십시오. Output 탭을 보면 실행 중인 두 Activity의 상태를 모니터링할 수 있습니다.

![Full Debug](./assets/MovieAnalytics/FullDebug.PNG "Full Debug")

Data Flow Activity 옆의 안경 모양 아이콘을 클릭하면 Data Flow 실행에 대한 자세한 내용을 볼 수 있습니다. Data Flow Activity는 약 1분 안에 완료되어야 합니다.

![Data Flow Monitoring](./assets/MovieAnalytics/DataFlowMonitoring.PNG "Data Flow monitoring")

이 실습에 설명된 것과 동일한 로직을 사용했다면 Data Flow가 SQL DW에 737개의 행을 썼을 것입니다. [SQL Server Management Studio (SSMS)](https://docs.microsoft.com/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-2017)로 이동하여 Pipeline이 올바르게 작동했는지 확인하고 무엇이 작성되었는지 볼 수 있습니다.

Pipeline이 작동하는 것을 확인한 후 변경 사항을 게시하십시오! Azure Data Factory를 계속 사용해보고 코드 없는 ETL 워크플로를 구축해 보십시오.
