const SUPER_ADMIN_ROLE = "super_admin";
const DASHBOARD_ROLES = ["viewer", "operator", SUPER_ADMIN_ROLE];
const NODE_ROUTE_ROLES = ["node", SUPER_ADMIN_ROLE];
const ADMIN_ROLES = [SUPER_ADMIN_ROLE];
const VALID_API_KEY_ROLES = ["viewer", "operator", "node", SUPER_ADMIN_ROLE];

function normalizeRole(role) {
  if (typeof role !== "string") {
    return "";
  }

  const normalizedRole = role.trim().toLowerCase();

  if (normalizedRole === "admin") {
    return SUPER_ADMIN_ROLE;
  }

  return normalizedRole;
}

function isRoleAllowed(role, allowedRoles = []) {
  const normalizedRole = normalizeRole(role);
  const normalizedAllowedRoles = allowedRoles.map(normalizeRole);

  return normalizedAllowedRoles.includes(normalizedRole);
}

module.exports = {
  ADMIN_ROLES,
  DASHBOARD_ROLES,
  NODE_ROUTE_ROLES,
  SUPER_ADMIN_ROLE,
  VALID_API_KEY_ROLES,
  isRoleAllowed,
  normalizeRole,
};
