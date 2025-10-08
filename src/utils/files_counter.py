import dxpy
import dxpy.exceptions


def count_all_files(project_id):
    try:
        # Check security context
        if not dxpy.SECURITY_CONTEXT:
            raise dxpy.exceptions.DXError("No authentication token found. Run 'dx login' or set DX_SECURITY_CONTEXT.")

        # Verify project_id
        dxpy.bindings.verify_string_dxid(project_id, expected_classes=["project", "container"])

        # Set project context
        dxpy.set_project_context(project_id)

        # Find all files in the project
        file_results = list(dxpy.find_data_objects(
            classname="file",
            project=project_id
        ))

        # Print the total number of files found
        file_count = len(file_results)
        print(f"Total number of files in project '{project_id}': {file_count}")

    except dxpy.exceptions.DXAPIError as api_error:
        print(f"DNAnexus API error: {api_error}")
        # List accessible projects for debugging
        print("\nListing projects you have access to:")
        try:
            projects = dxpy.find_projects()
            for project in projects:
                print(
                    f"Project ID: {project['id']}, Name: {dxpy.DXProject(project['id']).name}, Permission: {project['level']}")
        except Exception as e:
            print(f"Failed to list projects: {e}")
    except dxpy.exceptions.DXError as dx_error:
        print(f"DNAnexus error: {dx_error}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    project_id = "project-J3V3Gk8J2Vk49P5kqqVbb2Gx"
    count_all_files(project_id)